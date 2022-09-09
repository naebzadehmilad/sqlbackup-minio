#!/usr/bin/python3.8
import os, sys
import logging
import configparser
# from datetime import datetime
import time
import subprocess
from minio import Minio
from jinja2 import Template

import io

ts = time.localtime()
now = time.strftime("%Y-%m-%d-%H-%M-", ts)

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(levelname)s %(message)s', datefmt='%H:%M:%S')

conf = configparser.ConfigParser()


def config():
    global minutes
    global command
    global tar
    global rmtar
    global dbname
    global pathbackup
    global miniouser
    global miniopassword
    global miniohost
    global miniobucket
    global shop_prod
    global userdb
    global passdb
    global portdb
    global passdb
    global execute
    global hostdb
    global xtra
    global dump
    global minioactive
    global execute
    if os.path.exists('conf.cfg'):
        print('##config.cfg is exist##')
    else:
        conf.add_section('INFO')
        conf.set('INFO', 'info', 'command=###mysqldump or xtrabackup ######execute =systemctl or docker')
        conf.add_section('COMMAND')
        conf.set('COMMAND', 'command', 'mysqldump')

        conf.add_section('HOSTDB')
        conf.set('HOSTDB', 'hostdb', '127.0.0.1')
        conf.add_section('EXECUTE')
        conf.set('EXECUTE', 'execute', 'systemctl')
        conf.add_section('USERDB')
        conf.set('USERDB', 'userdb', 'root')
        conf.add_section('PASSDB')
        conf.set('PASSDB', 'passdb', 'SECRET')
        conf.add_section('PORTDB')
        conf.set('PORTDB', 'portdb', '3306')
        conf.add_section('DBNAME')
        conf.set('DBNAME', 'dbname', 'shop_prod')
        conf.add_section('MINIO')
        conf.set('MINIO', 'minio', 'true')
        conf.add_section('MINIO_HOST')
        conf.set('MINIO_HOST', 'minio_host', '192.168.10.93:9000')
        conf.add_section('MINIO_USER')
        conf.set('MINIO_USER', 'minio_user', 'minio')
        conf.add_section('MINIO_PASSWORD')
        conf.set('MINIO_PASSWORD', 'minio_password', 'miniopass')
        conf.add_section('MINIO_BUCKET')
        conf.set('MINIO_BUCKET', 'minio_bucket', 'miniobucketname')
        conf.add_section('PATH_BACKUP')
        conf.set('PATH_BACKUP', 'path_backup', '/opt/mysqldumps/')
        conf.add_section('DELETEBACKUPS')
        conf.set('DELETEBACKUPS','minutes','14400') #14400min =10days
        with open('conf.cfg', 'w') as configfile:
            conf.write(configfile)
        configfile.close()
        logging.debug('config was created, please check config')
        exit(1)
    conf.read('conf.cfg')
    command = conf.get('COMMAND', 'command')
    pathbackup = conf.get('PATH_BACKUP', 'path_backup')
    dbname = conf.get('DBNAME', 'dbname')
    userdb = conf.get('USERDB', 'userdb')
    passdb = conf.get('PASSDB', 'passdb')
    portdb = conf.get('PORTDB', 'portdb')
    execute = conf.get('EXECUTE', 'execute')
    hostdb = conf.get('HOSTDB', 'hostdb')
    miniouser = conf.get('MINIO_USER', 'minio_user')
    miniopassword = conf.get('MINIO_PASSWORD', 'minio_password')
    miniobucket = conf.get('MINIO_BUCKET', 'minio_bucket')
    miniohost = conf.get('MINIO_HOST', 'minio_host')
    minioactive = conf.get('MINIO', 'minio')
    minutes=conf.get('DELETEBACKUPS','minutes')
    execute=conf.get('EXECUTE','execute')

    isExist = os.path.exists(pathbackup)
    if not isExist: 
      # Create a new directory because it does not exist 
      os.makedirs(pathbackup)
      print(f"The {pathbackup} is created!")
    

    ##template>>>>>>>>>>>jinja2

    tm = Template(
        "[client] \nuser={{ dbuser }}\npassword={{dbpass}}\nport={{dbport}}\n[mysql]\nhost={{dbhost}}\ndatabase={{namedb}}")
    mycnf = tm.render(dbuser=userdb, dbpass=passdb, dbhost=hostdb, namedb=dbname, dbport=portdb)
    if os.path.exists('./.my.cnf'):
        print('##./.my.cnf is exist##')
    else:
        try:
            with open('./.my.cnf', 'w') as cnf:
                cnf.write(mycnf)
            cnf.close()
            subprocess.check_call('chmod 700 ./.my.cnf ; stat ./.my.cnf', shell=True, stdout=sys.stdout,
                                  stderr=subprocess.STDOUT)
            logging.info("./.my.cnf  created!")
        except:
            logging.error("can't create file .my.cnf")

    if command == 'mysqldump': 
        dump= f'{command} --defaults-extra-file=./.my.cnf  -v  --skip-opt --single-transaction --max_allowed_packet=1G {dbname}  > {pathbackup}{dbname}-{now}.sql '
        logging.info(dump)
        if minioactive == 'true':
            tar = f'tar -cvf /tmp/{dbname}-{now}.tar {pathbackup}{dbname}-{now}.sql '
            rmtar = f'rm -rf /tmp/{dbname}-{now}.tar '
    if command == 'xtrabackup': 
        dump= f'xtrabackup -u{userdb} -p{passdb} --backup   --target-dir={pathbackup}{dbname}-{now}.xtra --no-lock --host={hostdb} '
        logging.info(dump)
        if minioactive == 'true':
            tar = f'tar -cvf /tmp/{dbname}-{now}.xtra.tar {pathbackup}{dbname}-{now}.xtra '
            rmtar = f'rm -rf  /tmp/{dbname}-{now}.xtra.tar'
    


    else:
        logging.error('command Unknown!---------')



def backup():
    try:
        subprocess.check_call(dump, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
        logging.info('execute ' + dump)
        if minioactive == 'true' :
            subprocess.check_call(tar, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
            logging.info('execute ' + tar )
    except subprocess.CalledProcessError as e:
        logging.error(e.output)


def minio():
    client = Minio(
        endpoint=miniohost,
        access_key=miniouser,
        secret_key=miniopassword,
        secure=False
    )

    try:
        if command == 'xtrabackup' and minioactive == 'true' :
            fname = dbname+'-'+now+'.xtra.tar'
            fullp = '/tmp/'+dbname+'-'+now+'.xtra.tar'
            client.fput_object(miniobucket , fname,fullp , )
            logging.info("Upload Successfully..!")
            subprocess.check_call(rmtar, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
            logging.info(rmtar)
        if command == 'mysqldump' and minioactive == 'true' :
            fname = dbname+'-'+now+'.tar'
            fullp = '/tmp/'+dbname+'-'+now+'.tar'
            client.fput_object(miniobucket , fname,fullp , )
            subprocess.check_call(rmtar, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
            logging.info(rmtar)
    except subprocess.CalledProcessError as e:
        logging.error(e.output)


def rmoldidr():
    rmolddir = "find {pathbackup} -maxdepth 1 -mmin +{minutes} -type d  -print0 | xargs -0 rm -rf ".format(minutes=minutes,pathbackup=pathbackup)
    rmoldfiles = "find {pathbackup} -maxdepth 1 -mmin +{minutes} -type f -print0 | xargs -0 rm -rf ".format(minutes=minutes,pathbackup=pathbackup)
    if minutes != '0':
        try:
            subprocess.check_call(rmoldfiles, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
            logging.info('execute ' + rmoldfiles)
            subprocess.check_call(rmolddir, shell=True, stdout=sys.stdout, stderr=subprocess.STDOUT)
            logging.info('execute ' + rmolddir)
        except subprocess.CalledProcessError as e:
            logging.error(e.output)
config()
backup()
minio()
rmoldidr()
