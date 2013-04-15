# coding: utf-8

import os

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

@task
@roles('broker')
def download_broker_logs():
    log_filename = '/srv/pypln/logs/pypln-broker.out*'
    if env.host == 'fgv.pypln.org':
        log_filename = '/srv/pypln/wikipedia/logs/pypln-broker.out*'
    if not os.path.exists('logs'):
        os.mkdir('logs')
    machine = env.host.split('.')[0]
    logs = run('ls -1 {}'.format(log_filename))
    for log_filename in logs.split('\n'):
        log_filename = log_filename.strip()
        tail = log_filename.split('.out')[1].replace('.', '')
        if tail:
            tail = '-' + tail
        new_name = 'broker-{}{}.log'.format(machine, tail)
        local_filename = os.path.join('logs', new_name)
        get(log_filename, local_filename)
    local('cat logs/broker-{0}*.log > logs/{0}-broker.log'.format(machine))
    local('rm logs/broker-{}*.log'.format(machine))

@task
@roles('web')
def check_progress(corpus=""):
    put("check_progress.py", "/srv/pypln/project/web/pypln/web/")
    with prefix("source /srv/pypln/project/bin/activate"), \
            cd("/srv/pypln/project/web/pypln/web/"):
        pythonpath = "/srv/pypln/project/web/pypln/web/apps/:$PYTHONPATH"
        run("PYTHONPATH={} DJANGO_SETTINGS_MODULE=settings.production python "
            "check_progress.py {}".format(pythonpath, corpus))
