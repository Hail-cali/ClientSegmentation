import findspark; findspark.init()
import pyspark
import os
from utils import Config
import datetime
import json

class Pattern:
    label_name = ['soft_watching', 'regular_watching', 'binge_watching', 'heavy_watching']

    def __init__(self, read_day, week_cnt, type='total'):
        self.read_day =read_day
        self.week_cnt = week_cnt
        self.type = type
        # print(f':: Pattern Type : {self.type} | read_day per week : {self.read_day} | read_cnt per week : {self.week_cnt}')
        if type == 'soft':
            self.func = self.soft
        elif type == 'hard':
            self.func = self.hard
        elif type == 'binge':
            self.func = self.binge
        elif type == 'regular':
            self.func = self.regular
        else:
            self.func = self.total

    def __call__(self, x):

        return self.func(x)

    def soft(self, x):
        if x[1][0] < self.read_day and x[1][1] < self.week_cnt:
            return x

    def regular(self, x):
        if x[1][0] >= self.read_day and x[1][1] < self.week_cnt:
            return x

    def hard(self,x):
        if x[1][0] >=self.read_day and x[1][1] >= self.week_cnt:
            return x

    def binge(self, x):
        if x[1][0] == 1 and x[1][1] >= self.week_cnt:
            return x

    def total(self,x):
        return x


class PattenCluster:

    data_path = ['buy_data.csv', 'category_data.csv', 'read_data.csv', 'user_data.csv']

    def __init__(self, root, c_name='Hail', num_worker=3):
        self.root = root
        self.num_worker = num_worker
        conf = pyspark.SparkConf().set('spark.driver.host','127.0.0.1')
        self.sc = pyspark.SparkContext('local[*]', appName=c_name, conf=conf).getOrCreate()
        self._load_data()

    def rdd(self, file):
        map = {'read':2, 'buy':0, 'cat':1, 'uid':3}
        # .map(lambda x: str(x.encode('utf-8')))
        return self.sc.textFile(os.path.join(self.root, '../db', self.data_path[map[file]]), self.num_worker, use_unicode=True)

    def make_combined_batch(self, opt='read', batch_num=0):
        '''

        :param opt: csv file opt
        :param batch_num: 0~ len(self.window)  : start window
        :return: batch reduced by category feature(drop cat, reduce data to one)
        '''
        if batch_num >= len(self.window):
            print(f'--------> End Iter, return None')
            return

        header, rdd = self._make_rdd(opt=opt)
        start = self.window[::self.wd][batch_num]
        end = self.window[::self.wd][batch_num+1]
        batch =rdd.filter(lambda x: x[header['create_date']] >= start and x[header['create_date']] < end )


        if opt == 'buy':
            return self._buy_batch_filter(batch, header)

        batch_user=batch.map(lambda x:
                  ((x[header['sec_id']],x[header['create_date']]),
                   (int(x[header['read_day']]), int(x[header['read_cnt']]))
                   )). \
            reduceByKey(lambda a,b: (max(a[0], b[0]), a[1]+b[1])). \
            sortBy(lambda x: (-1*x[1][1], x[1][0]))

        description = f'( (sec_id, create_date), (read_day, read_cnt) )'

        return batch_user, description

    def mean_batch(self, rdd):
        '''

        :param rdd: batch from make_combined_batch
        :return: mean batch info
        '''
        combined_window = self._batch_combine_by_window(rdd)

        mean_batch = combined_window.map(lambda x: (None, (x[1][0], x[1][1], x[1][2], 1))).\
            reduceByKey(lambda a, b : [a_+b_ for a_, b_ in zip(a,b)]).\
            mapValues(lambda x:  [round(x_hat/x[3], 1) for x_hat in x[:-1]] + [x[3]]).\
            map(lambda x: x[1]+ [round(x[1][1]/x[1][0])])

        description = f'mean: (  read_day per window({self.wd}),  read_cnt per window({self.wd}), week per window({self.wd}) | len(data) , read_cnt per day  )'

        # print(description)
        return mean_batch


    def mean_batch_merge(self, rdd1, rdd2):
        '''

        :param rdd1: read rdd
        :param rdd2: buy rdd
        :return: mean batch rdd ( will be added , now only sum vlaues)
        '''
        combined_window = self._batch_combine_by_window(rdd1)
        combined_window_buy = self._batch_combine_by_window_buy(rdd2)

        merge_rdd =combined_window.join(combined_window_buy)
        return merge_rdd.map(lambda x: (None,(x[1][0][0],x[1][0][1],x[1][0][2], x[1][1]))). \
            reduceByKey(lambda a,b:[a_+b_ for a_, b_ in zip(a,b)] )

    def _buy_batch_filter(self,rdd, header):
        batch_user=rdd.map(lambda x:
                             ((x[header['sec_id']],x[header['create_date']]),
                              (int(x[header['real_cash']]))
                              )). \
            reduceByKey(lambda a,b: ( a[1]+b[1])). \
            sortBy(lambda x: (-1*x[1]))

        description = f'( (sec_id, create_date), (real_cash) )'

        return batch_user, description


    def __make_batch(self, rdd, header, batch_num=0):
        '''

        :param rdd: rdd without head
        :param header: header
        :param batch_num: 0~ len(self.window)  : start window
        :return: batch in window slide
        '''
        if batch_num >= len(self.window):
            print(f'End Iter, return None')
            return
        start = self.window[::self.wd][batch_num]
        end = self.window[::self.wd][batch_num+1]
        batch =rdd.filter(lambda x: x[header['create_date']] >= start and x[header['create_date']] < end )
        return batch

    # combine batch with window
    def _batch_combine_by_window(self, rdd):
        '''

        :param rdd: batch_user from make_combined_batch
        :return: combine batch by merge within slide window
        '''
        return rdd.map(lambda x: ((x[0][0]),( x[1][0], x[1][1], 1 )) ). \
            reduceByKey(lambda a, b : [a_+b_ for a_, b_ in zip(a,b)])

    def _batch_combine_by_window_buy(self,rdd):
        return  rdd.map(lambda x: ((x[0][0]),( x[1] )) ). \
            reduceByKey(lambda a, b : a+b)

    def _load_data(self):
        if os.path.exists(self.root):
            config = Config(json_path=str(os.path.join(self.root, '../service.config.json')))
            self.wd = config.wd
            self.num_classes = config.num_classes
            self.batch_size = config.batch_size

            get = self.get_sources
            if not get:
                get = self.make_sources

            load = Config(json_path=str(os.path.join(self.root, '../log', get[-1])))
            self.log = load
            # self.window = load.window

    @property
    def window(self):
        return self.log.window


    @property
    def get_sources(self):
        return os.listdir(os.path.join(self.root, '../log'))

    @property
    def check_expired(self):
        'if old token, update token with make_sources methode'
        return False

    @property
    def make_sources(self):
        import pandas as pd
        is_read = pd.read_csv(os.path.join(self.root, '../db/read_data.csv'))
        total_window = sorted(is_read.create_date.unique())

        save_log = {'window': total_window,
                    'date': datetime.date.today().strftime('%d.%m.%y'),
                    'batch':[]}

        fname = 'log_' + datetime.date.today().strftime('%d.%m.%y') + '.json'
        file_path = os.path.join(self.root, '../log', fname)
        with open(file_path, 'w') as f:
            json.dump(save_log, f)
        return self.get_sources

    def close_cluster(self):
        print(f':: BY HAIL ::')
        self.sc.stop()

    def _make_rdd(self, opt='read'):
        '''

        :param opt:
        :return: rdd without head
        '''
        rdd = self.rdd(opt)
        rdd = rdd.map(lambda x: x.split(','))
        header_raw = rdd.first()
        header = {v:k for k,v  in enumerate(header_raw)}
        rdd = rdd.filter(lambda row : row != header_raw)

        return header, rdd