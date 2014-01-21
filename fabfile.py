from fabric.api import run, local, lcd, put, env

env.hosts = ['root@de5']

def deploy_tracker():
	run('stop sunrise-tracker')
	with lcd('./tracker'):
		local('go build tracker.go')
		put('tracker', '/usr/local/bin/sunrise-tracker')
	run('start sunrise-tracker')

def deploy_worker():
	with lcd('./worker'):
		local('go build worker.go')
		put('worker', '/home/sun/worker_new')

