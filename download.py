import argparse
import requests
import logging
import json
from tqdm import tqdm
from sshtunnel import SSHTunnelForwarder
from datetime import datetime
from database.amr_info import AmrRawData
from database.sessions import create_session


class Logger:
    def __init__(self, debug_file='debug.log', name='main', level='DEBUG', format='[%(asctime)s] [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S') -> None:
        self.logger = logging.getLogger(name=name)
        self.logger.setLevel(self.getLevel(level))
        self.logger.propagate = False
        
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
            
        self.file_logger = logging.FileHandler(debug_file, mode='a')
        self.file_logger.setLevel(self.getLevel(level))
        self.file_logger.setFormatter(logging.Formatter(format))

        self.stream_logger = logging.StreamHandler()
        self.stream_logger.setLevel(self.getLevel(level))
        self.stream_logger.setFormatter(logging.Formatter(format))

        self.logger.addHandler(self.file_logger)
        self.logger.addHandler(self.stream_logger)

    def getLevel(self, level_string):
        if level_string == 'DEBUG':
            return logging.DEBUG
        elif level_string == 'INFO':
            return logging.INFO
        elif level_string == 'WARN':
            return logging.WARN
        elif level_string == 'WARNING':
            return logging.WARNING
        elif level_string == 'ERROR':
            return logging.ERROR
        elif level_string == 'CRITICAL':
            return logging.CRITICAL

    @classmethod
    def debug(cls, messages):
        cls().logger.debug(messages)

    @classmethod
    def info(cls, messages):
        cls().logger.info(messages)

    @classmethod
    def warn(cls, messages):
        cls().logger.warn(messages)

    @classmethod
    def error(cls, messages):
        cls().logger.error(messages, exc_info=True)

    @classmethod
    def critical(cls, messages):
        cls().logger.critical(messages)
        

class ImageDownloader:
    def __init__(self,  **kwargs) -> None:
        self.kwargs = kwargs
        self.server = None

    def _get_configs(self):
        with open('configs.json', 'r') as f:
            configs = json.load(f)
        return configs

    def _get_url(self, ssh_tunnel, image_pool):
        if ssh_tunnel['ssh_address_or_host'] and ssh_tunnel['ssh_username'] and ssh_tunnel['ssh_password']:
            self.server = SSHTunnelForwarder(
                ssh_address_or_host=(ssh_tunnel['ssh_address_or_host'], 22),
                ssh_username=ssh_tunnel['ssh_username'],
                ssh_password=ssh_tunnel['ssh_password'],
                remote_bind_address=(image_pool['ip'], image_pool['port'])
            )
            self.server.start()
            local_host = '127.0.0.1'
            local_port = self.server.local_bind_port
        else:
            local_host = image_pool['ip']
            local_port = image_pool['port']
        
        download_url_prefix = image_pool['donwload_url_prefix']
        
        return f'http://{local_host}:{local_port}/{download_url_prefix}'

    def _get_ssh_tunnel_settings(self, configs, site):
        return configs[site]['SSHTUNNEL']

    def _get_database_settings(self, configs, site):
        return configs[site]['database']
    
    def _get_image_list(self, ssh_tunnel, database, image_pools, site, start_date, end_date, **kwargs):
        Logger.info(f'Getting image list from {site} site {kwargs["line_id"]} line')
        with create_session(ssh_tunnel, database) as session:
            query = session.query(AmrRawData).filter(AmrRawData.create_time.between(start_date, end_date))
            if kwargs['group_type_list']:
                if kwargs['group_type']:
                    del kwargs['group_type']
                query = query.filter(AmrRawData.group_type.in_(kwargs['group_type_list']))
            if kwargs['component_preffix_name']:
                if kwargs['comp_name']:
                    del kwargs['comp_name']
                query = query.filter(AmrRawData.comp_name.like(f'{kwargs["component_preffix_name"]}%'))
            if kwargs['part_number_list']:
                if kwargs['part_number']:
                    del kwargs['part_number']
                query = query.filter(AmrRawData.part_number.in_(kwargs['part_number_list']))

            for key, value in kwargs.items():
                if value and key in AmrRawData.__table__.columns.keys():
                    query = query.filter(getattr(AmrRawData, key) == value)
            
            if kwargs['limit']:
                query = query.limit(kwargs['limit'])

            data = query.all()
            
            Logger.info(f'Found {len(data)} images from {site} site {kwargs["line_id"]} line')
            
        images_list = []
        
        for obj in data:
            if site == 'NK':
                images_list.append(f'images/{image_pools[obj.line_id]["line_ip"]}/{obj.image_path}')
            elif site == 'ZJ':
                images_list.append(f'{obj.line_id}/{obj.image_path}')
            else:
                images_list.append(obj.image_path)

        return images_list

    def download(self, site, line_id, image_list, download_name, url):
        Logger.info(f'Downloading {len(image_list)} images from {site} site {line_id} line')
        Logger.info(f'Download URL: {url}')

        download_proxies = {'http': url}
        response = requests.post(url, proxies=download_proxies, json={
            "paths": image_list
        })
        if response.status_code == 200:
            total_size = len(response.content)
            block_size = 1024
            with tqdm(
                total=total_size, 
                unit='B', 
                unit_scale=True, 
                unit_divisor=1024, 
                desc=f'[{datetime.now().strftime("%Y-%m-%d %H:%M:%S,%f")[:-3]}] [INFO] Downloading {download_name}.zip',
            ) as bar:
                with open(f'{download_name}.zip', 'wb') as zip_file:
                    for data in response.iter_content(block_size):
                        zip_file.write(data)
                        bar.update(len(data))
                    
            Logger.info('Download completed')
            
            if total_size != 0 and total_size != bar.n:
                Logger.error('Download failed')

    def run(self):
        configs = self._get_configs()
        ssh_tunnel = self._get_ssh_tunnel_settings(configs, self.kwargs['site'])
        database = self._get_database_settings(configs, self.kwargs['site'])
        image_pools = configs[self.kwargs['site']]['image_pool']
        if self.kwargs['line_id']:
            line_id = self.kwargs['line_id']
            url = self._get_url(ssh_tunnel, image_pools[line_id])
            images_list = self._get_image_list(ssh_tunnel, database, image_pools, **self.kwargs)
            self.download(self.kwargs['site'], line_id, images_list, f'{self.kwargs["download_name"]}_{line_id}', url)
            if self.server:
                self.server.stop()
        elif self.kwargs['line_id_list'] == []:
            for line_id in image_pools:
                self.kwargs['line_id']  = line_id
                url = self._get_url(ssh_tunnel, image_pools[line_id])
                images_list = self._get_image_list(ssh_tunnel, database, image_pools, **self.kwargs)
                self.download(self.kwargs['site'], line_id, images_list, f'{self.kwargs["download_name"]}_{line_id}', url)
                if self.server:
                    self.server.stop()
        else:
            for line_id in self.kwargs['line_id_list']:
                self.kwargs['line_id'] = line_id
                url = self._get_url(ssh_tunnel, image_pools[line_id])
                images_list = self._get_image_list(ssh_tunnel, database, image_pools, **self.kwargs)
                self.download(self.kwargs['site'], line_id, images_list, f'{self.kwargs["download_name"]}_{line_id}', url)
                if self.server:
                    self.server.stop()

def parse_opt():
    parser = argparse.ArgumentParser(description='Download images from image pool')
    parser.add_argument('--product-name', help='Product name')
    parser.add_argument('--site', help='Site name e.g. JQ, ZJ, NK, HZ, etc.', required=True)
    parser.add_argument('--line-id', help='Line ID', default='')
    parser.add_argument('--station-id', help='Station ID')
    parser.add_argument('--factory', help='Factory name')
    parser.add_argument('--aoi-id', help='AOI ID')
    parser.add_argument('--top-btm', help='Top or bottom side')
    parser.add_argument('--imulti-col', help='IMulti column')
    parser.add_argument('--imulti-row', help='IMulti row')
    parser.add_argument('--carrier-sn', help='Carrier SN')
    parser.add_argument('--board-sn', help='Board SN')
    parser.add_argument('--image-path', help='Image path')
    parser.add_argument('--image-name', help='Image name')
    parser.add_argument('--part-number', help='Part number')
    parser.add_argument('--comp-name', help='Component name')
    parser.add_argument('--window-id', help='Window ID')
    parser.add_argument('--aoi-defect', help='AOI defect')
    parser.add_argument('--op-defect', help='OP defect')
    parser.add_argument('--ai-result', help='AI result')
    parser.add_argument('--center-x', help='Center X')
    parser.add_argument('--center-y', help='Center Y')
    parser.add_argument('--region-x', help='Region X')
    parser.add_argument('--region-y', help='Region Y')
    parser.add_argument('--angle', help='Angle')
    parser.add_argument('--cycle-time', help='Cycle time')
    parser.add_argument('--total-comp', help='Total component')
    parser.add_argument('--package-type', help='Package type')
    parser.add_argument('--comp-type', help='Component type')
    parser.add_argument('--group-type', help='Group type')
    parser.add_argument('--is-covered', action='store_true')
    parser.add_argument('--limit', type=int, help='Limit')
    parser.add_argument('--start-date', help='Start date, e.g. 2021-01-01', required=True)
    parser.add_argument('--end-date', default=datetime.now().strftime('%Y-%m-%d'), help='End date, e.g. 2021-01-01', required=True)
    parser.add_argument('--group-type-list', nargs='+', help='Group type list')
    parser.add_argument('--part-number-list', nargs='+', help='Part Number List')
    parser.add_argument('--component-preffix-name', help='Component preffix name, e.g. L2002, FL6602 etc.')
    parser.add_argument('--line-id-list', nargs='+', help='Line ID list', default=[])
    parser.add_argument('--download-name', help='Download name', default='images')
    opt = parser.parse_args()

    return opt


if __name__ == '__main__':
    opt = parse_opt()
    image_downloader = ImageDownloader(**vars(opt))
    image_downloader.run()