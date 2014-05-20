#!/usr/bin/python
import sys, os
import errno
import fileinput

# CLI (connector to Hadoop executable) to run n-gram (n in a range of values) Hadoop job
# Author: byron@admin.grnet.gr

def make_sure_path_exists(path):
    try:
        os.makedirs(path)
    except OSError as exception:
        if exception.errno != errno.EEXIST:
            raise

def parse_arguments(args):
    """ Parse command-line arguments, get values """
    from optparse import OptionParser

    kw = {}
    kw["usage"] = "%prog [options]"
    kw["description"] = \
        "%prog configures and runs a Hadoop n-gram (with a value range for n) computation job."

    parser = OptionParser(**kw)
    parser.disable_interspersed_args()

    parser.add_option("--from",
                      action="store", type="int", dest="start",		# opts.from is n/a
                      help="The starting value for n",
                      default=2)

    parser.add_option("--to",
                      action="store", type="int", dest="to",
                      help="The ending value for n",
                      default=2)

    parser.add_option("--reducers",
                      action="store", type="int", dest="reduce",
                      help="Number of reduce tasks",
                      default=1)

    parser.add_option("--threshold",
                      action="store", type="int", dest="threshold",
                      help="Threshold to discard #frequencies",
                      default=1)

    parser.add_option("--inputdir",
                      action="store", type="string", dest="inputdir",
                      help="Input HDFS directory",
                      default="wiki/in")

    (opts, args) = parser.parse_args(args)

    # Verify arguments

    if opts.start < 2:
        print >> sys.stderr, "Invalid From argument."
        parser.print_help()
        sys.exit(1)

    if opts.to < 2:
        print >> sys.stderr, "Invalid To argument."
        parser.print_help()
        sys.exit(1)

    if opts.reduce < 0:
        print >> sys.stderr, "Invalid Reduce argument."
        parser.print_help()
        sys.exit(1)

    if opts.threshold < -2:
        print >> sys.stderr, "Invalid Threshold argument."
        parser.print_help()
        sys.exit(1)

    if opts.start > opts.to: 
        print >> sys.stderr, "From argument must be less or equal then To argument."
        parser.print_help()
        sys.exit(1)

    return (opts, args)


def main():
    (opts, args) = parse_arguments(sys.argv[1:])
    numreducers = "-D mapred.reduce.tasks="+str(opts.reduce)
    if opts.start == opts.to: opts.to = opts.to+1

    # create and run the Hadoop job. This single job calculates *all* n-grams for multiple values of n (from...to) by prefixing the 'n' value before the key
    n = opts.start
    run = "hadoop fs -rmr /user/hduser/wiki/ngram"+str(n)+"; hadoop jar java/NGramJobIMC.jar NGramJobIMC "+opts.inputdir+"  /user/hduser/wiki/ngram"+str(n)+" "+str(opts.start)+" "+str(opts.to)
    print run
    result = os.system(run)
    print "Hadoop result =", result
    if result != 0: sys.exit(3)

    # Hadoop job to discard frequencies<threshold
    run = "hadoop jar java/ThresholdJob.jar ThresholdJob wiki/ngram"+str(n)+" wiki/temp "+str(opts.threshold)
    print run
    result = os.system(run)
    print "Hadoop result =", result
    if result != 0: sys.exit(4)

    if opts.start+1 >= opts.to:    # do this only if there is only one single value for n
        # get the results from HDFS into the local filesystem  
        out_fname = "n"+ "0" * (len(str(opts.to))-len(str(opts.start))) + str(opts.start)+".sort" 
        run = "hadoop fs -cat /user/hduser/wiki/temp/p* | sort -r -n -k "+str(n+1)+ " > "+out_fname
        print run
        os.system(run)
    else:
        # get the results from HDFS into the local filesystem  
        temp_dir = './part-m'
        make_sure_path_exists(temp_dir)
        run = "rm "+temp_dir+"/*"
        print run
        os.system(run)
        run = "hadoop fs -get /user/hduser/wiki/temp/p* "+temp_dir
        print run
        os.system(run)

        filens = []	# list of part-m-* map files
        filensh = [None] * (opts.to-opts.start)	# list of file handles for writing same-length ngrams
        for i in range (opts.start, opts.to):
            filens.append("./n"+str(i))
        mapfiles = os.listdir(temp_dir)
        for i in range(0, len(mapfiles)):	# create the filenames for output files
            mapfiles[i] = temp_dir+"/"+mapfiles[i]
        for i in range (0, opts.to-opts.start):	# open the file handles (for writing)
            filensh[i] = open(filens[i], "w")
        print sorted(mapfiles)
        for fn in sorted(mapfiles):
            if os.path.isfile(fn):
                maph = open(fn, "r")
                line = maph.readline()
                while line:
                    ind = line.index("_")
                    n = int(line[0:ind])
                    filensh[n-opts.start].write(line[ind+1:])	# redirect line to correct file according to prefix
                    line = maph.readline()
                maph.close()
        for fh in filensh:
            fh.close()
        for n in range(opts.start, opts.to):
            print "n=", n
            curr_fname = "./n"+str(n)
            run = "sort -r -n -k "+str(n+1)+" "+curr_fname+" > "+curr_fname+".sort; rm "+curr_fname
            print run
            os.system(run)

    # delete HDFS results directory
    run = "hadoop fs -rmr /user/hduser/wiki/ngram"+str(opts.start)
    print run
    os.system(run)

    # delete temp results directory
    run = "hadoop fs -rmr /user/hduser/wiki/temp"
    print run
    os.system(run)

if __name__ == "__main__":
    sys.exit(main())
