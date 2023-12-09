import argparse

def parse_opt():
    parser = argparse.ArgumentParser(prog='setting API')
    #############################
    #       Base setting        #
    #############################
    parser.add_argument('--readme', default='', type=str)
    parser.add_argument('--tasks', default='seg', type=str, help='view | seg')
    parser.add_argument('--root', default='./', type=str, help='root path for log')
    parser.add_argument('--num_worker', default=3, type=int, help='number of worker')
    parser.add_argument('--cluster_name', default='HAIL.local', type=str, help='spark-client host name')

    args = parser.parse_args()

    return args