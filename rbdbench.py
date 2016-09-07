#!/usr/bin/python2.6
import multiprocessing
import rados
import rbd
from datetime import datetime
from datetime import timedelta 
import logging
import optparse
import random
import string

logging.basicConfig(level=logging.INFO, format='%(message)s')

parser = optparse.OptionParser()
parser.add_option("-c", "--conf", action="store", dest="cephconf", type="string", default="/etc/ceph/ceph.conf",
    help="""Ceph cluster config file (defaults to /etc/ceph/ceph.conf)""")
parser.add_option("-p", "--pool", action="store", dest="cephpool", type="string", default="rbd",
    help="""Ceph cluster pool name (defaults to rdb)""")
parser.add_option("-n", "--name", action="store", dest="diskname", type="string", default=''.join(random.choice(string.ascii_uppercase + string.digits) for x in range(20)),
    help="""Disk name (defaults to random)""")
parser.add_option("-d", "--disksize", action="store", dest="disksize", type="int", default="1024",
    help="""Disk size in MB""")
parser.add_option("-o", "--order", action="store", dest="objectorder", type="int", default="22",
    help="""Object size order. 22 = 4M, 21=2M, 20=1M, 19=512k, 18=256k, 17=128k, 16=64k.""")
parser.add_option("-t", "--threads", action="store", dest="threads", type="int", default="1",
    help="""Number of threads. Should be even, so 1 2 4 6 8.""")

options, args = parser.parse_args()

#  b=X//Y ; c=X-((Y-1)*b) for the last
# but check for blocksize divisability eg 4M

cluster = rados.Rados(conffile=options.cephconf)
try:
    cluster.connect()
    ioctx = cluster.open_ioctx(options.cephpool)
    try:
        rbd_inst = rbd.RBD()
        size = options.disksize * 1024**2
	order = options.objectorder
	# order 22 = 4M, 21=2M, 20=1M, 19=512k, 18=256k, 17=128k, 16=64k

	#  b=X//Y ; c=X-((Y-1)*b) for the last
	# but check for blocksize divisability eg 4M
	blocksizemost=size//options.threads
	blocksizelast=size-((options.threads-1)*blocksizemost)
	logging.info("Threads %s, threads-1 times %s, 1 time %s" % (options.threads, blocksizemost, blocksizelast))

	logging.info("Creating image with size %sMB, order %s" % (options.disksize, options.objectorder))
        rbd_inst.create(ioctx, options.diskname, size, order=order, old_format=False,features=0)
        image = rbd.Image(ioctx, options.diskname)
        try:
	    logging.info("Creating ram object equal to disk size")
            data = '\1' * options.disksize * 1024**2
            writestarttime=datetime.now()
	    logging.info("Write starting at %s" % writestarttime)
            image.write(data,0)
            image.close()
	    del data
            writeendtime=datetime.now()
	    writetime = writeendtime - writestarttime
            writeelapsedtime = writetime.seconds + ( writetime.microseconds / 1E6 )
	    logging.info("Writing took %ss at %s MB/s" % (writeelapsedtime, options.disksize/writeelapsedtime))
	    image = rbd.Image(ioctx, options.diskname)
            readstarttime=datetime.now()
	    logging.info("Read starting at %s" % readstarttime)
            data = image.read(0,options.disksize * 1024**2)
            image.close()
            readendtime=datetime.now()
	    readtime = readendtime - readstarttime
            readelapsedtime = readtime.seconds + ( readtime.microseconds / 1E6 )
	    logging.info("Reading took %ss at %s MB/s" % (readelapsedtime, options.disksize/readelapsedtime))
        finally:
	    logging.info("Removing image %s" % options.diskname)
	    rbd_inst.remove(ioctx, options.diskname)
            logging.info("RBDBench: Disk/data size: %sMB, W=%sMB/s, R=%sMB/s, order=%s, threads=%s" % (options.disksize, options.disksize/writeelapsedtime, options.disksize/readelapsedtime, options.objectorder, options.threads))
    finally:
        ioctx.close()
finally:
    cluster.shutdown()
