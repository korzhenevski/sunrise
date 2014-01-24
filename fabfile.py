from fabric.api import run, local, lcd, put, env

env.hosts = ['root@de5']

def deploy_tracker():
	run('stop sunrise-tracker')
	with lcd('./trackerd'):
		local('go build trackerd.go')
		put('trackerd', '/usr/local/bin/sunrise-trackerd')
	run('start sunrise-tracker')

def deploy_worker():
	with lcd('./workerd'):
		local('go build workerd.go')
		put('workerd', '/home/sun/worker_new')

