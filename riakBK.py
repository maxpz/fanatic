import fabric.api as fab
from fabric.api import warn_only
from fabric.network import disconnect_all
from contextlib import contextmanager
from fabric import exceptions
from boto.ec2.connection import EC2Connection
from boto.regioninfo import *
import time, smtplib, sys, re

riak_nodes = {'i-093b3556968621ce1':'ec2-18-237-187-100.us-west-2.compute.amazonaws.com',
'i-0dcb91f948182b2fb':'ec2-18-236-237-132.us-west-2.compute.amazonaws.com',
'i-01c2310f94a2ac4f4':'ec2-54-185-111-130.us-west-2.compute.amazonaws.com',
'i-04feaf8211823ae4a':'ec2-54-218-101-78.us-west-2.compute.amazonaws.com',
'i-06a328c0d67896cdd':'ec2-54-218-112-196.us-west-2.compute.amazonaws.com',
'i-0c81c57db49c1e508':'ec2-52-37-192-126.us-west-2.compute.amazonaws.com',
'i-0e51568ba2ac76b5a':'ec2-52-12-61-92.us-west-2.compute.amazonaws.com',
'i-014275837bf1ba684':'ec2-18-237-167-62.us-west-2.compute.amazonaws.com',
'i-07935b2a72b463912':'ec2-54-189-76-218.us-west-2.compute.amazonaws.com',
'i-0a8bfbce5c8fd6684':'ec2-34-211-52-227.us-west-2.compute.amazonaws.com'}


@contextmanager
def ssh(settings):
	with settings:
		try:
			yield
		finally:
			disconnect_all()


def ssh1(host, user, ssh_key, command):
    with ssh(fab.settings(host_string=host, user=user, key_filename=ssh_key, warn_only=True)):
        return fab.sudo(command, pty=False)

def ebs_snapshot(instance_id):
	access_key = 'AKIAIUI7VUQMPVY5JXOA'
	secret_key = '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'
	snapshotname = 'Riak Backup - ' + instance_id + '- ' + time.strftime("%Y-%m-%d %H:%M:%S")
	try:
		ec2_region = boto.ec2.get_region(aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name='us-west-2')
		print 'Connected Successfully!'
	except Exception, e:
		print e
		print 'Connection failed!'
	
	ec2_conn = boto.ec2.connection.EC2Connection(
    aws_access_key_id=access_key, 
    aws_secret_access_key=secret_key,
    region=ec2_region)	
	volumes = ec2_conn.get_all_volumes(volume_ids=None, filters=None)
	instance_volumes = [v for v in volumes if v.attach_data.instance_id == instance_id]
	for vol in instance_volumes:
		snapshot = ec2_conn.create_snapshot(vol.id, snapshotname)


def sendmail(subject, text):
	user = "mperez@spartanapproach.com"
	password = "Man891004ux"
	FROM = "mperez@spartanapproach.com"
	TO = ['mperez@spartanapproach.com']
	
	message = """\From: %s\nTo: %s\nSubject: %s\n\n%s
	""" % (FROM, ", ".join(TO), subject, text)
	try:
		server = smtplib.SMTP("smtp.gmail.com", 587)
		server.ehlo()
		server.starttls()
		server.login(user, password)
		server.sendmail(FROM, TO, message)
		server.close()
	except Exception, e:
		print e	


def stop_riak(host, user, ssh_key):
	command = "/etc/init.d/monit stop"
	output = ssh1(host, user, ssh_key, command)
	if not 'Stopping monit' in output:
		subject = "Alert! Monit Stop Error"
		text = "Monit was not stopped %s. Terminating the backup script!" %host
		print text
		#sendmail(subject, text)
		sys.exit()
	command = "riak stop"
	output = ssh1(host, user, ssh_key, command)
	if not 'ok' in output:
		subject = "Alert! Riak Backup Error"
		text = "Riak service failed to STOP on host %s. Terminating the backup script!" %host
		print text
		#sendmail(subject, text)
		sys.exit()

def start_riak(host, user, ssh_key):
	command = "riak start"
	ssh1(host, user, ssh_key, command)
	#run command again to verify if node is running
	output = ssh1(host, user, ssh_key, command)
	output.replace('\n', ' ').replace('\r', '')	
	if not 'Node is already running!' in output:
		subject = "Alert! Riak backup Error"
		text = "Riak Service failed to START on host %s. Terminating the backup script!" %host
		print text
		#sendmail(subject, text)
		sys.exit()
	# Wait for riak_kv service to start
	ssh1(host, user, ssh_key, 'riak-admin wait-for-service riak_kv')

def check_primary_vnodes(host, user, ssh_key):
	command = "riak-admin transfers"
	
	while True:
		output = ssh1(host, user, ssh_key, command)
		output = output.replace('\n', ' ').replace('\r', '')
		p = re.compile('does not have \d+ primary partitions running')
		m = p.match(output)
		if m:
			time.sleep(15)
		else:
			break

def check_handoffs(host, user, ssh_key):
	command  = "riak-admin transfers"
		
	while True:
			output = ssh1(host, user,ssh_key, command)
			output = output.replace('\n', ' ').replace('\r', '')
			if 'waiting to handoff' in output:
				time.sleep(15)
			else:
				break
	
try:
	for instance, node in riak_nodes.iteritems():
		instance_id = instance
		host = node
		user = 'ubuntu'
		ssh_key = 'data-nodes.pem'
		# stop riak services
		stop_riak(host, user, ssh_key)
		# take snapshot		
		ebs_snapshot(instance_id)
		# start riak services and wait for kv service to come online
		start_riak(host, user, ssh_key)
		#check if any primary vnodes are down and wait till they are up
		check_primary_vnodes(host, user, ssh_key)
		# wait till handoffs are done
		check_handoffs(host, user, ssh_key)
	#sendmail("Alert! Riak Backup Status", "Riak Backup script has executed successfully!")
except Exception as e:
    print "Exception :",str(e)