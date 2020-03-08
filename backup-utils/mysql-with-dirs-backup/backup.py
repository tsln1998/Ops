#!/usr/bin/env python3
from subprocess import Popen, PIPE
from os import remove, getcwd  # , rename
from shutil import move as rename
from os.path import exists, getmtime, join as path_join, getsize as file_size, basename, dirname
from time import strftime, localtime, time as current_time
from datetime import datetime as dt
from json import load as json_load
from tempfile import gettempdir
from qiniu import Auth as QnAuth, put_file as qn_put_file, etag as qn_etag

# disable increment backup
NO_INCREMENT_BACKUP = None
# disable full backup in sunday
NO_FULL_BACKUP_BY_WEEK = None
# backup storage path
STORAGE_PATH = None

DIRECTORY_CONFIG = None
MYSQL_CONFIG = None

# SYSTEM
_TAR_FLAG = '.backup_flag'
_CONFIG = None
_ORPHAN_FILES = []


def log(*args):
    print(format_time(current_time(), '[%Y-%m-%d %H:%M:%S]'), *args)


def generate_path(fn, is_temp=True):
    p = path_join(gettempdir() if is_temp else STORAGE_PATH, fn)
    if is_temp:
        _ORPHAN_FILES.append(p)
    return p


def need_full_backup(default=True):
    return (not NO_INCREMENT_BACKUP) and (default or (dt.now().weekday() == 6 and not NO_FULL_BACKUP_BY_WEEK))


def get_tar_flag_date(path):
    return format_time(getmtime(path))


def format_time(t, f='%Y%m%d%H%M%S'):
    return strftime(f, t if not isinstance(t, float) else localtime(t))


def backup_directory(backup_name, path):
    flag_path = path_join(path, _TAR_FLAG)
    current_date = format_time(current_time())
    # default rule
    is_full_backup = need_full_backup(not exists(flag_path))
    # remove backup flag
    if is_full_backup and exists(flag_path):
        remove(flag_path)
    # format backup name
    backup_path = generate_path('%s-%s-%s.tar.gz' %
                                (backup_name, current_date, "full" if is_full_backup else "increment"))
    # backup by tar
    p = Popen(['tar', '-g', _TAR_FLAG, '-zcvf', backup_path, path], cwd=path, stderr=PIPE, stdout=PIPE)
    if p.wait() != 0:
        raise Exception('tar backup failed')
    assert exists(backup_path)
    # check backup tar contains any valid file
    backup_has_valid_files = 0 < len(list(filter(
        lambda fn: not fn.endswith('/') and not fn.endswith('/%s' % _TAR_FLAG),
        map(
            lambda bs: bs.decode().rstrip('\n'),
            p.stdout.readlines()
        )
    )))
    # success
    if backup_has_valid_files:
        log("backup directory %s success." % backup_name)
        upload_backup(backup_path)
    else:
        log("skip empty backup %s." % backup_name)


def backup_database(config, databases):
    databases = databases.split(',')
    # generate file path
    storage_path = generate_path("%s.sql" % ('_'.join(databases)), False)
    export_path = generate_path("%s-%s.sql" % ('_'.join(databases), format_time(current_time())))
    export_patch = generate_path("%s.sql.%s.patch" % ('_'.join(databases), format_time(current_time())))
    # mysql dump to file
    with open(export_path, 'w') as export_file:
        p = Popen([
            'mysqldump',
            '-u%s' % config['username'],
            '-p%s' % config['password'],
            '-h%s' % config['host'],
            '-P%s' % config['port'],
            '--skip-comments',
            '--skip-extended-insert',
            '--hex-blob',
            '--events',
            ' '.join(databases),
        ], stdout=export_file)
        if p.wait() != 0:
            raise Exception('Can\'t backup database %s' % ','.join(databases))
    # choose diff or full sql
    if need_full_backup(not exists(storage_path)):
        upload_backup(export_path)
    else:
        with open(export_patch, 'w') as patch_file:
            p = Popen(['diff', '-Nur', storage_path, export_path], stdout=patch_file)
            if p.wait() != 0:
                raise Exception('Can\'t diff %s <-> %s', basename(storage_path), basename(export_patch))
        if file_size(export_patch) == 0:
            log("skip empty database backup %s." % ','.join(databases))
            remove(export_patch)
        else:
            log("backup database %s success." % ','.join(databases))
            upload_backup(export_patch)
    # update last sql file
    if exists(storage_path):
        remove(storage_path)
    rename(export_path, storage_path)


# upload the backup file to cloud
def upload_backup(path):
    if 'qiniu' in _CONFIG:
        access_key, secret_key, bucket_name = \
            _CONFIG['qiniu']['access_key'], _CONFIG['qiniu']['secret_key'], _CONFIG['qiniu']['bucket_name']
        if access_key is not None and secret_key is not None and bucket_name is not None:
            qn = QnAuth(access_key, secret_key)
            key = basename(path)
            ret, info = qn_put_file(qn.upload_token(bucket_name, key, 120), key, path)
            if ret['key'] != key or ret['hash'] != qn_etag(path):
                raise Exception('Upload to KODO failed')
            print('Upload file %s to KODO success.' % key)
    raise Exception('Upload file %s to cloud failed' % basename(path))


if __name__ == '__main__':
    with open(path_join(dirname(__file__), 'config.json'), 'r') as cf:
        _CONFIG = json_load(cf)
        # global config
        STORAGE_PATH = _CONFIG['storage_path'] if 'storage_path' in _CONFIG else getcwd()
        NO_INCREMENT_BACKUP = _CONFIG['no_increment_backup'] if 'no_increment_backup' in _CONFIG else False
        NO_FULL_BACKUP_BY_WEEK = _CONFIG['no_full_backup_by_week'] if 'no_full_backup_by_week' in _CONFIG else False
        # db config
        MYSQL_CONFIG = _CONFIG['mysql']
        if 'databases' not in MYSQL_CONFIG:
            MYSQL_CONFIG['databases'] = _CONFIG['databases']
        # directory config
        DIRECTORY_CONFIG = _CONFIG['dirs'] if 'dirs' in _CONFIG else _CONFIG['directory']
    for name in DIRECTORY_CONFIG:
        backup_directory(name, DIRECTORY_CONFIG[name])
    for db in MYSQL_CONFIG['databases']:
        backup_database(MYSQL_CONFIG, db)
    for orphan in _ORPHAN_FILES:
        if exists(orphan):
            remove(orphan)
