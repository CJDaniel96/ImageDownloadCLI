# coding: utf-8
from sqlalchemy import Boolean, Column, DateTime, Float, Integer, Text
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AmrRawData(Base):
    __tablename__ = 'amr_raw_data'
    __table_args__ = {'schema': 'public'}

    uuid = Column(Text, primary_key=True, nullable=False)
    product_name = Column(Text)
    site = Column(Text, primary_key=True, nullable=False)
    line_id = Column(Text)
    station_id = Column(Text)
    factory = Column(Text, primary_key=True, nullable=False)
    aoi_id = Column(Text)
    create_time = Column(DateTime, primary_key=True, nullable=False)
    top_btm = Column(Text)
    imulti_col = Column(Integer)
    imulti_row = Column(Integer)
    carrier_sn = Column(Text)
    board_sn = Column(Text)
    image_path = Column(Text)
    image_name = Column(Text)
    part_number = Column(Text)
    comp_name = Column(Text)
    window_id = Column(Integer)
    aoi_defect = Column(Text)
    op_defect = Column(Text)
    ai_result = Column(Text, comment='1:OK(URD)   0:NG(IRI)')
    center_x = Column(Integer)
    center_y = Column(Integer)
    region_x = Column(Integer)
    region_y = Column(Integer)
    angle = Column(Float)
    cycle_time = Column(Float)
    total_comp = Column(Integer)
    package_type = Column(Text)
    comp_type = Column(Text)
    group_type = Column(Text)
    is_covered = Column(Boolean)
