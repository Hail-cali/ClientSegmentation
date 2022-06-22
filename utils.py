import json
import pandas as pd


class Config:
    def __init__(self, json_path):

        with open(json_path, mode='r') as io:
            params = json.loads(io.read())
        self.__dict__.update(params)

    def save(self, json_path):
        with open(json_path, mode='w') as io:
            json.dump(self.__dict__, io, indent=4)

    def update(self, json_path):
        with open(json_path, mode='r') as io:
            params = json.loads(io.read())
        self.__dict__.update(params)

    def update_log(self, dict_params):
        self.__dict__.update(dict_params)

    @property
    def dict(self):
        return self.__dict__


class Graph:

    def __init__(self):
       pass

    def __call__(self, log):
        col_name = ['TOTAL_READ_DAY', 'TOTAL_READ_CNT', 'READ_WEEK','UNIQUE USER','READ_CNT(DAY)', 'cash', 'TYPE', 'batch']
        in_batch_ls = []
        for k in log.dict:
            if k not in ['window', 'date']:
                if len(k.split('_')) == 3:
                    in_batch_ls.append(log.dict[k] +[k.split('_')[-1][:5]] + [int(k.split('_')[1])])
        in_batch_df = pd.DataFrame(in_batch_ls,columns=col_name)

        return in_batch_df

    def make_pivot(self, df, col=['TYPE','batch', 'TOTAL_READ_DAY']):
        return df.pivot(col)

