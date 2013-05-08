# coding: utf-8

import os
import shutil

from fabric.api import task, hosts, run, env, get, put, roles, local, prefix, cd


env.roledefs.update({
        'router': ['pypln@hpc.pypln.org'],
        'broker': ['pypln@dirrj.pypln.org', 'pypln@hpc.pypln.org',
                   'pypln@fgv.pypln.org'],
        'pipeliner': ['pypln@hpc.pypln.org'],
        'web': ['pypln@wikipedia.pypln.org'],
})


@task
@roles('router', 'broker', 'pipeliner', 'web')
def stop_services():
    if env.host == 'fgv.pypln.org':
        service_name = 'wikipedia-broker'
    elif env.host == 'wikipedia.pypln.org':
        service_name = 'pypln-web'
    elif env.host == 'hpc.pypln.org':
        service_name = 'pypln-router pypln-pipeliner pypln-broker'
    elif env.host == 'dirrj.pypln.org':
        service_name = 'pypln-broker'
    run('supervisorctl stop {}'.format(service_name))

@task
@roles('router', 'broker', 'pipeliner', 'web')
def remove_logs():
    log_path = '/srv/pypln/logs/*'
    if env.host == 'fgv.pypln.org':
        log_path = '/srv/pypln/wikipedia/logs/*'
    run('rm -rf {}'.format(log_path))


@task
@roles('router')
def start_router():
    run('supervisorctl start pypln-router')


@task
@roles('pipeliner')
def start_pipeliner():
    run('supervisorctl start pypln-pipeliner')


@task
@roles('broker')
def start_brokers():
    service_name = 'pypln-broker'
    if env.host == 'fgv.pypln.org':
        service_name = 'wikipedia-broker'
    run('supervisorctl start {}'.format(service_name))


@task
@roles('web')
def start_web():
    run('supervisorctl start pypln-web')

def mkdir_if_not_exists(path):
    if not os.path.exists(path):
        os.mkdir(path)

@task
@roles('broker')
def download_broker_logs():
    machine = env.host.split('.')[0]
    mkdir_if_not_exists('logs')
    try:
        shutil.rmtree('logs/{}'.format(machine))
    except OSError:
        pass
    mkdir_if_not_exists('logs/{}'.format(machine))
    log_filename = '/srv/pypln/logs/pypln-broker.out'
    if env.host == 'fgv.pypln.org':
        log_filename = '/srv/pypln/wikipedia/logs/pypln-broker.out'
    log_directory = os.path.dirname(log_filename)
    log_filename = os.path.basename(log_filename)

    log_tarball = 'broker-logs.tar.gz'
    with cd(log_directory):
        # first, old logs
        run('rm -f {}'.format(log_tarball))
        run('tar -zcf {} {}'.format(log_tarball, log_filename + '.*'))
        remote_filename = os.path.join(log_directory, log_tarball)
        get(remote_filename, 'logs/{}/{}'.format(machine, log_tarball))
        run('rm -f {}'.format(log_tarball))

        get(log_filename, 'logs/{}/'.format(machine))

        local('cd logs/{0}/ && tar xfz {1} && cd ..'
              .format(machine, log_tarball))
        local('cat logs/{0}/*.out* > logs/{0}-broker.log'.format(machine))
        local('rm -rf logs/{}/'.format(machine))

def upload_and_run_inside_django_env(filename, args=''):
    put(filename, "/srv/pypln/project/web/pypln/web/")
    with prefix("source /srv/pypln/project/bin/activate"), \
            cd("/srv/pypln/project/web/pypln/web/"):
        pythonpath = "/srv/pypln/project/web/pypln/web/apps/:$PYTHONPATH"
        run("PYTHONPATH={} DJANGO_SETTINGS_MODULE=settings.production python "
            "{} {}".format(pythonpath, filename, args))

@task
@roles('web')
def check_progress(corpus=""):
    upload_and_run_inside_django_env('check_progress.py', corpus)

@task
@roles('web')
def calculate_pos_size(corpus="ptwp"):
    upload_and_run_inside_django_env('calculate_pos_size.py', corpus)

@task
@roles('web')
def check_uploads(corpus="ptwp"):
    upload_and_run_inside_django_env('check_uploads.py', corpus)

@task
@roles('web')
def export(corpus="ptwp"):
    put('sqlite_corpus.py', "/srv/pypln/project/web/pypln/web/")
    upload_and_run_inside_django_env('export_to_sqlite.py', corpus)
