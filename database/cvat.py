# coding: utf-8
from sqlalchemy import BigInteger, Boolean, CheckConstraint, Column, DateTime, Float, ForeignKey, Integer, SmallInteger, String, Text, UniqueConstraint, text
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class AuthGroup(Base):
    __tablename__ = 'auth_group'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_group_id_seq'::regclass)"))
    name = Column(String(150), nullable=False, unique=True)


class AuthUser(Base):
    __tablename__ = 'auth_user'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_user_id_seq'::regclass)"))
    password = Column(String(128), nullable=False)
    last_login = Column(DateTime(True))
    is_superuser = Column(Boolean, nullable=False)
    username = Column(String(150), nullable=False, unique=True)
    first_name = Column(String(150), nullable=False)
    last_name = Column(String(150), nullable=False)
    email = Column(String(254), nullable=False)
    is_staff = Column(Boolean, nullable=False)
    is_active = Column(Boolean, nullable=False)
    date_joined = Column(DateTime(True), nullable=False)


class DjangoContentType(Base):
    __tablename__ = 'django_content_type'
    __table_args__ = (
        UniqueConstraint('app_label', 'model'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('django_content_type_id_seq'::regclass)"))
    app_label = Column(String(100), nullable=False)
    model = Column(String(100), nullable=False)


class DjangoMigration(Base):
    __tablename__ = 'django_migrations'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('django_migrations_id_seq'::regclass)"))
    app = Column(String(255), nullable=False)
    name = Column(String(255), nullable=False)
    applied = Column(DateTime(True), nullable=False)


class DjangoSession(Base):
    __tablename__ = 'django_session'

    session_key = Column(String(40), primary_key=True, index=True)
    session_data = Column(Text, nullable=False)
    expire_date = Column(DateTime(True), nullable=False, index=True)


class DjangoSite(Base):
    __tablename__ = 'django_site'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('django_site_id_seq'::regclass)"))
    domain = Column(String(100), nullable=False, unique=True)
    name = Column(String(50), nullable=False)


class EngineTrainingproject(Base):
    __tablename__ = 'engine_trainingproject'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_trainingproject_id_seq'::regclass)"))
    host = Column(String(256), nullable=False)
    username = Column(String(256), nullable=False)
    password = Column(String(256), nullable=False)
    training_id = Column(String(64), nullable=False)
    enabled = Column(Boolean)
    project_class = Column(String(2))


class SocialaccountSocialapp(Base):
    __tablename__ = 'socialaccount_socialapp'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('socialaccount_socialapp_id_seq'::regclass)"))
    provider = Column(String(30), nullable=False)
    name = Column(String(40), nullable=False)
    client_id = Column(String(191), nullable=False)
    secret = Column(String(191), nullable=False)
    key = Column(String(191), nullable=False)


class AccountEmailaddres(Base):
    __tablename__ = 'account_emailaddress'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('account_emailaddress_id_seq'::regclass)"))
    email = Column(String(254), nullable=False, unique=True)
    verified = Column(Boolean, nullable=False)
    primary = Column(Boolean, nullable=False)
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    user = relationship('AuthUser')


class AuthPermission(Base):
    __tablename__ = 'auth_permission'
    __table_args__ = (
        UniqueConstraint('content_type_id', 'codename'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_permission_id_seq'::regclass)"))
    name = Column(String(255), nullable=False)
    content_type_id = Column(ForeignKey(
        'django_content_type.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    codename = Column(String(100), nullable=False)

    content_type = relationship('DjangoContentType')


class AuthUserGroup(Base):
    __tablename__ = 'auth_user_groups'
    __table_args__ = (
        UniqueConstraint('user_id', 'group_id'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_user_groups_id_seq'::regclass)"))
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)
    group_id = Column(ForeignKey('auth_group.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    group = relationship('AuthGroup')
    user = relationship('AuthUser')


class AuthtokenToken(Base):
    __tablename__ = 'authtoken_token'

    key = Column(String(40), primary_key=True, index=True)
    created = Column(DateTime(True), nullable=False)
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, unique=True)

    user = relationship('AuthUser', uselist=False)


class DjangoAdminLog(Base):
    __tablename__ = 'django_admin_log'
    __table_args__ = (
        CheckConstraint('action_flag >= 0'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('django_admin_log_id_seq'::regclass)"))
    action_time = Column(DateTime(True), nullable=False)
    object_id = Column(Text)
    object_repr = Column(String(200), nullable=False)
    action_flag = Column(SmallInteger, nullable=False)
    change_message = Column(Text, nullable=False)
    content_type_id = Column(ForeignKey(
        'django_content_type.id', deferrable=True, initially='DEFERRED'), index=True)
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    content_type = relationship('DjangoContentType')
    user = relationship('AuthUser')


class EngineCloudstorage(Base):
    __tablename__ = 'engine_cloudstorage'
    __table_args__ = (
        UniqueConstraint('provider_type', 'resource', 'credentials'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_cloudstorage_id_seq'::regclass)"))
    provider_type = Column(String(20), nullable=False)
    resource = Column(String(63), nullable=False)
    display_name = Column(String(63), nullable=False)
    created_date = Column(DateTime(True), nullable=False)
    updated_date = Column(DateTime(True), nullable=False)
    credentials = Column(String(500), nullable=False)
    credentials_type = Column(String(29), nullable=False)
    specific_attributes = Column(String(50), nullable=False)
    description = Column(Text, nullable=False)
    owner_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)

    owner = relationship('AuthUser')


class EngineProfile(Base):
    __tablename__ = 'engine_profile'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_profile_id_seq'::regclass)"))
    rating = Column(Float(53), nullable=False)
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, unique=True)

    user = relationship('AuthUser', uselist=False)


class EngineProject(Base):
    __tablename__ = 'engine_project'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_project_id_seq'::regclass)"))
    name = Column(String(256), nullable=False)
    bug_tracker = Column(String(2000), nullable=False)
    created_date = Column(DateTime(True), nullable=False)
    updated_date = Column(DateTime(True), nullable=False)
    status = Column(String(32), nullable=False)
    assignee_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    owner_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    training_project_id = Column(ForeignKey(
        'engine_trainingproject.id', deferrable=True, initially='DEFERRED'), index=True)

    assignee = relationship(
        'AuthUser', primaryjoin='EngineProject.assignee_id == AuthUser.id')
    owner = relationship(
        'AuthUser', primaryjoin='EngineProject.owner_id == AuthUser.id')
    training_project = relationship('EngineTrainingproject')


class SocialaccountSocialaccount(Base):
    __tablename__ = 'socialaccount_socialaccount'
    __table_args__ = (
        UniqueConstraint('provider', 'uid'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('socialaccount_socialaccount_id_seq'::regclass)"))
    provider = Column(String(30), nullable=False)
    uid = Column(String(191), nullable=False)
    last_login = Column(DateTime(True), nullable=False)
    date_joined = Column(DateTime(True), nullable=False)
    extra_data = Column(Text, nullable=False)
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    user = relationship('AuthUser')


class SocialaccountSocialappSite(Base):
    __tablename__ = 'socialaccount_socialapp_sites'
    __table_args__ = (
        UniqueConstraint('socialapp_id', 'site_id'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('socialaccount_socialapp_sites_id_seq'::regclass)"))
    socialapp_id = Column(ForeignKey('socialaccount_socialapp.id',
                          deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    site_id = Column(ForeignKey('django_site.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    site = relationship('DjangoSite')
    socialapp = relationship('SocialaccountSocialapp')


class AccountEmailconfirmation(Base):
    __tablename__ = 'account_emailconfirmation'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('account_emailconfirmation_id_seq'::regclass)"))
    created = Column(DateTime(True), nullable=False)
    sent = Column(DateTime(True))
    key = Column(String(64), nullable=False, unique=True)
    email_address_id = Column(ForeignKey(
        'account_emailaddress.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    email_address = relationship('AccountEmailaddres')


class AuthGroupPermission(Base):
    __tablename__ = 'auth_group_permissions'
    __table_args__ = (
        UniqueConstraint('group_id', 'permission_id'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_group_permissions_id_seq'::regclass)"))
    group_id = Column(ForeignKey('auth_group.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    permission_id = Column(ForeignKey(
        'auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    group = relationship('AuthGroup')
    permission = relationship('AuthPermission')


class AuthUserUserPermission(Base):
    __tablename__ = 'auth_user_user_permissions'
    __table_args__ = (
        UniqueConstraint('user_id', 'permission_id'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('auth_user_user_permissions_id_seq'::regclass)"))
    user_id = Column(ForeignKey('auth_user.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)
    permission_id = Column(ForeignKey(
        'auth_permission.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    permission = relationship('AuthPermission')
    user = relationship('AuthUser')


class EngineDatum(Base):
    __tablename__ = 'engine_data'
    __table_args__ = (
        CheckConstraint('chunk_size >= 0'),
        CheckConstraint('image_quality >= 0'),
        CheckConstraint('size >= 0'),
        CheckConstraint('start_frame >= 0'),
        CheckConstraint('stop_frame >= 0')
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_data_id_seq'::regclass)"))
    chunk_size = Column(Integer)
    size = Column(Integer, nullable=False)
    image_quality = Column(SmallInteger, nullable=False)
    start_frame = Column(Integer, nullable=False)
    stop_frame = Column(Integer, nullable=False)
    frame_filter = Column(String(256), nullable=False)
    compressed_chunk_type = Column(String(32), nullable=False)
    original_chunk_type = Column(String(32), nullable=False)
    storage_method = Column(String(15), nullable=False)
    storage = Column(String(15), nullable=False)
    cloud_storage_id = Column(ForeignKey(
        'engine_cloudstorage.id', deferrable=True, initially='DEFERRED'), index=True)

    cloud_storage = relationship('EngineCloudstorage')


class SocialaccountSocialtoken(Base):
    __tablename__ = 'socialaccount_socialtoken'
    __table_args__ = (
        UniqueConstraint('app_id', 'account_id'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('socialaccount_socialtoken_id_seq'::regclass)"))
    token = Column(Text, nullable=False)
    token_secret = Column(Text, nullable=False)
    expires_at = Column(DateTime(True))
    account_id = Column(ForeignKey('socialaccount_socialaccount.id',
                        deferrable=True, initially='DEFERRED'), nullable=False, index=True)
    app_id = Column(ForeignKey('socialaccount_socialapp.id',
                    deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    account = relationship('SocialaccountSocialaccount')
    app = relationship('SocialaccountSocialapp')


class EngineClientfile(Base):
    __tablename__ = 'engine_clientfile'
    __table_args__ = (
        UniqueConstraint('data_id', 'file'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_clientfile_id_seq'::regclass)"))
    file = Column(String(1024), nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)

    data = relationship('EngineDatum')


class EngineImage(Base):
    __tablename__ = 'engine_image'
    __table_args__ = (
        CheckConstraint('frame >= 0'),
        CheckConstraint('height >= 0'),
        CheckConstraint('width >= 0')
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_image_id_seq'::regclass)"))
    path = Column(String(1024), nullable=False)
    frame = Column(Integer, nullable=False)
    height = Column(Integer, nullable=False)
    width = Column(Integer, nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)

    data = relationship('EngineDatum')


class EngineRemotefile(Base):
    __tablename__ = 'engine_remotefile'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_remotefile_id_seq'::regclass)"))
    file = Column(String(1024), nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)

    data = relationship('EngineDatum')


class EngineServerfile(Base):
    __tablename__ = 'engine_serverfile'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_serverfile_id_seq'::regclass)"))
    file = Column(String(1024), nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)

    data = relationship('EngineDatum')


class EngineTask(Base):
    __tablename__ = 'engine_task'
    __table_args__ = (
        CheckConstraint('overlap >= 0'),
        CheckConstraint('segment_size >= 0')
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_task_id_seq'::regclass)"))
    name = Column(String(256), nullable=False)
    mode = Column(String(32), nullable=False)
    created_date = Column(DateTime(True), nullable=False)
    updated_date = Column(DateTime(True), nullable=False)
    status = Column(String(32), nullable=False)
    bug_tracker = Column(String(2000), nullable=False)
    owner_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    overlap = Column(Integer)
    assignee_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    segment_size = Column(Integer, nullable=False)
    project_id = Column(ForeignKey('engine_project.id',
                        deferrable=True, initially='DEFERRED'), index=True)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)
    dimension = Column(String(2), nullable=False)
    subset = Column(String(64), nullable=False)

    assignee = relationship(
        'AuthUser', primaryjoin='EngineTask.assignee_id == AuthUser.id')
    data = relationship('EngineDatum')
    owner = relationship(
        'AuthUser', primaryjoin='EngineTask.owner_id == AuthUser.id')
    project = relationship('EngineProject')


class DatasetRepoGitdatum(EngineTask):
    __tablename__ = 'dataset_repo_gitdata'

    task_id = Column(ForeignKey('engine_task.id', deferrable=True,
                     initially='DEFERRED'), primary_key=True)
    url = Column(String(2000), nullable=False)
    path = Column(String(256), nullable=False)
    sync_date = Column(DateTime(True), nullable=False)
    status = Column(String(20), nullable=False)
    lfs = Column(Boolean, nullable=False)


class EngineVideo(Base):
    __tablename__ = 'engine_video'
    __table_args__ = (
        CheckConstraint('height >= 0'),
        CheckConstraint('width >= 0')
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_video_id_seq'::regclass)"))
    path = Column(String(1024), nullable=False)
    height = Column(Integer, nullable=False)
    width = Column(Integer, nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), unique=True)

    data = relationship('EngineDatum', uselist=False)


class EngineLabel(Base):
    __tablename__ = 'engine_label'
    __table_args__ = (
        UniqueConstraint('task_id', 'name'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_label_id_seq'::regclass)"))
    name = Column(String(64), nullable=False)
    task_id = Column(ForeignKey('engine_task.id',
                     deferrable=True, initially='DEFERRED'), index=True)
    color = Column(String(8), nullable=False)
    project_id = Column(ForeignKey('engine_project.id',
                        deferrable=True, initially='DEFERRED'), index=True)

    project = relationship('EngineProject')
    task = relationship('EngineTask')


class EngineRelatedfile(Base):
    __tablename__ = 'engine_relatedfile'
    __table_args__ = (
        UniqueConstraint('data_id', 'path'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_relatedfile_id_seq'::regclass)"))
    path = Column(String(1024), nullable=False)
    data_id = Column(ForeignKey('engine_data.id',
                     deferrable=True, initially='DEFERRED'), index=True)
    primary_image_id = Column(ForeignKey(
        'engine_image.id', deferrable=True, initially='DEFERRED'), index=True)

    data = relationship('EngineDatum')
    primary_image = relationship('EngineImage')


class EngineSegment(Base):
    __tablename__ = 'engine_segment'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_segment_id_seq'::regclass)"))
    start_frame = Column(Integer, nullable=False)
    stop_frame = Column(Integer, nullable=False)
    task_id = Column(ForeignKey('engine_task.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    task = relationship('EngineTask')


class EngineTrainingprojectimage(Base):
    __tablename__ = 'engine_trainingprojectimage'
    __table_args__ = (
        CheckConstraint('idx >= 0'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_trainingprojectimage_id_seq'::regclass)"))
    idx = Column(Integer, nullable=False)
    training_image_id = Column(String(64), nullable=False)
    task_id = Column(ForeignKey('engine_task.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    task = relationship('EngineTask')


class EngineAttributespec(Base):
    __tablename__ = 'engine_attributespec'
    __table_args__ = (
        UniqueConstraint('label_id', 'name'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_attributespec_id_seq'::regclass)"))
    label_id = Column(ForeignKey('engine_label.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    default_value = Column(String(128), nullable=False)
    input_type = Column(String(16), nullable=False)
    mutable = Column(Boolean, nullable=False)
    name = Column(String(64), nullable=False)
    values = Column(String(4096), nullable=False)

    label = relationship('EngineLabel')


class EngineJob(Base):
    __tablename__ = 'engine_job'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_job_id_seq'::regclass)"))
    segment_id = Column(ForeignKey('engine_segment.id', deferrable=True,
                        initially='DEFERRED'), nullable=False, index=True)
    assignee_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    status = Column(String(32), nullable=False)
    reviewer_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)

    assignee = relationship(
        'AuthUser', primaryjoin='EngineJob.assignee_id == AuthUser.id')
    reviewer = relationship(
        'AuthUser', primaryjoin='EngineJob.reviewer_id == AuthUser.id')
    segment = relationship('EngineSegment')


class EngineTrainingprojectlabel(Base):
    __tablename__ = 'engine_trainingprojectlabel'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_trainingprojectlabel_id_seq'::regclass)"))
    training_label_id = Column(String(64), nullable=False)
    cvat_label_id = Column(ForeignKey(
        'engine_label.id', deferrable=True, initially='DEFERRED'), nullable=False, index=True)

    cvat_label = relationship('EngineLabel')


class EngineJobcommit(Base):
    __tablename__ = 'engine_jobcommit'
    __table_args__ = (
        CheckConstraint('version >= 0'),
    )

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_jobcommit_id_seq'::regclass)"))
    version = Column(Integer, nullable=False)
    timestamp = Column(DateTime(True), nullable=False)
    message = Column(String(4096), nullable=False)
    author_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)

    author = relationship('AuthUser')
    job = relationship('EngineJob')


class EngineLabeledimage(Base):
    __tablename__ = 'engine_labeledimage'
    __table_args__ = (
        CheckConstraint('"group" >= 0'),
        CheckConstraint('frame >= 0')
    )

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledimage_id_seq'::regclass)"))
    frame = Column(Integer, nullable=False)
    group = Column(Integer)
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)
    label_id = Column(ForeignKey('engine_label.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    source = Column(String(16))

    job = relationship('EngineJob')
    label = relationship('EngineLabel')


class EngineLabeledshape(Base):
    __tablename__ = 'engine_labeledshape'
    __table_args__ = (
        CheckConstraint('"group" >= 0'),
        CheckConstraint('frame >= 0')
    )

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledshape_id_seq'::regclass)"))
    frame = Column(Integer, nullable=False)
    group = Column(Integer)
    type = Column(String(16), nullable=False)
    occluded = Column(Boolean, nullable=False)
    z_order = Column(Integer, nullable=False)
    points = Column(Text, nullable=False)
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)
    label_id = Column(ForeignKey('engine_label.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    source = Column(String(16))

    job = relationship('EngineJob')
    label = relationship('EngineLabel')


class EngineLabeledtrack(Base):
    __tablename__ = 'engine_labeledtrack'
    __table_args__ = (
        CheckConstraint('"group" >= 0'),
        CheckConstraint('frame >= 0')
    )

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledtrack_id_seq'::regclass)"))
    frame = Column(Integer, nullable=False)
    group = Column(Integer)
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)
    label_id = Column(ForeignKey('engine_label.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    source = Column(String(16))

    job = relationship('EngineJob')
    label = relationship('EngineLabel')


class EngineReview(Base):
    __tablename__ = 'engine_review'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_review_id_seq'::regclass)"))
    estimated_quality = Column(Float(53), nullable=False)
    status = Column(String(16), nullable=False)
    assignee_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)
    reviewer_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)

    assignee = relationship(
        'AuthUser', primaryjoin='EngineReview.assignee_id == AuthUser.id')
    job = relationship('EngineJob')
    reviewer = relationship(
        'AuthUser', primaryjoin='EngineReview.reviewer_id == AuthUser.id')


class EngineIssue(Base):
    __tablename__ = 'engine_issue'
    __table_args__ = (
        CheckConstraint('frame >= 0'),
    )

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_issue_id_seq'::regclass)"))
    frame = Column(Integer, nullable=False)
    position = Column(Text, nullable=False)
    created_date = Column(DateTime(True), nullable=False)
    resolved_date = Column(DateTime(True))
    job_id = Column(ForeignKey('engine_job.id', deferrable=True,
                    initially='DEFERRED'), nullable=False, index=True)
    owner_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    resolver_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    review_id = Column(ForeignKey('engine_review.id',
                       deferrable=True, initially='DEFERRED'), index=True)

    job = relationship('EngineJob')
    owner = relationship(
        'AuthUser', primaryjoin='EngineIssue.owner_id == AuthUser.id')
    resolver = relationship(
        'AuthUser', primaryjoin='EngineIssue.resolver_id == AuthUser.id')
    review = relationship('EngineReview')


class EngineLabeledimageattributeval(Base):
    __tablename__ = 'engine_labeledimageattributeval'

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledimageattributeval_id_seq'::regclass)"))
    value = Column(String(4096), nullable=False)
    spec_id = Column(ForeignKey('engine_attributespec.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)
    image_id = Column(ForeignKey('engine_labeledimage.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    image = relationship('EngineLabeledimage')
    spec = relationship('EngineAttributespec')


class EngineLabeledshapeattributeval(Base):
    __tablename__ = 'engine_labeledshapeattributeval'

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledshapeattributeval_id_seq'::regclass)"))
    value = Column(String(4096), nullable=False)
    spec_id = Column(ForeignKey('engine_attributespec.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)
    shape_id = Column(ForeignKey('engine_labeledshape.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    shape = relationship('EngineLabeledshape')
    spec = relationship('EngineAttributespec')


class EngineLabeledtrackattributeval(Base):
    __tablename__ = 'engine_labeledtrackattributeval'

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_labeledtrackattributeval_id_seq'::regclass)"))
    value = Column(String(4096), nullable=False)
    spec_id = Column(ForeignKey('engine_attributespec.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)
    track_id = Column(ForeignKey('engine_labeledtrack.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    spec = relationship('EngineAttributespec')
    track = relationship('EngineLabeledtrack')


class EngineTrackedshape(Base):
    __tablename__ = 'engine_trackedshape'
    __table_args__ = (
        CheckConstraint('frame >= 0'),
    )

    type = Column(String(16), nullable=False)
    occluded = Column(Boolean, nullable=False)
    z_order = Column(Integer, nullable=False)
    points = Column(Text, nullable=False)
    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_trackedshape_id_seq'::regclass)"))
    frame = Column(Integer, nullable=False)
    outside = Column(Boolean, nullable=False)
    track_id = Column(ForeignKey('engine_labeledtrack.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    track = relationship('EngineLabeledtrack')


class EngineComment(Base):
    __tablename__ = 'engine_comment'

    id = Column(Integer, primary_key=True, server_default=text(
        "nextval('engine_comment_id_seq'::regclass)"))
    message = Column(Text, nullable=False)
    created_date = Column(DateTime(True), nullable=False)
    updated_date = Column(DateTime(True), nullable=False)
    author_id = Column(ForeignKey(
        'auth_user.id', deferrable=True, initially='DEFERRED'), index=True)
    issue_id = Column(ForeignKey('engine_issue.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)

    author = relationship('AuthUser')
    issue = relationship('EngineIssue')


class EngineTrackedshapeattributeval(Base):
    __tablename__ = 'engine_trackedshapeattributeval'

    id = Column(BigInteger, primary_key=True, server_default=text(
        "nextval('engine_trackedshapeattributeval_id_seq'::regclass)"))
    value = Column(String(4096), nullable=False)
    shape_id = Column(ForeignKey('engine_trackedshape.id', deferrable=True,
                      initially='DEFERRED'), nullable=False, index=True)
    spec_id = Column(ForeignKey('engine_attributespec.id', deferrable=True,
                     initially='DEFERRED'), nullable=False, index=True)

    shape = relationship('EngineTrackedshape')
    spec = relationship('EngineAttributespec')
