import findspark;
findspark.init() # need to set spark client api for python
from pattern import pattenSpark
import pattern
import os
import datetime
from pattern.util.parser import parse_opt

def pattern_spark_test(args):

    cluster = pattenSpark.PattenCluster(root=args.root, c_name=args.cluster_name, num_worker=args.num_worker)

    print(f'Main Protocol in SPARK RUN :: CLUSTER : {cluster.sc}')
    log_save_path = os.path.join(cluster.root, 'log', 'log_' + datetime.date.today().strftime('%d.%m.%y') + '.json')
    print(f'\t:: DEPLOY INFO :: \t')
    print(f'\t:: Save log path {log_save_path}')
    print(f'\t:: DATE {cluster.log.date}')
    print(f'\t:: WINDOW SIZE {cluster.wd}')
    print(f'\t:: WINDOW PERIOD {cluster.window[0]} - {cluster.window[-1]}')
    print(f"{'-' * 30}\n Batch Mean Segementation By HAIL \n{'-' * 30}")
    for i in range(len(cluster.window[::cluster.wd]) - 2):

        batch_name = f'batch_{i}'
        print(f"{'=' * 10} Batch {i} Segmenation Start {'=' * 10}")

        batch_combine, description = cluster.make_combined_batch(opt='read', batch_num=i)
        batch_buy, description_buy = cluster.make_combined_batch(opt='buy', batch_num=i)

        total_info = cluster.mean_batch(batch_combine).collect()
        total_info = total_info[0]
        cluster.log.update_log({batch_name: total_info})

        # segementation
        pat = ['soft', 'hard', 'regular', 'binge']
        batch_week_cnt = round(total_info[1] / total_info[2])
        batch_read_day = round(total_info[0] / total_info[2])
        print(f" BASE INFO |  batch_week_cnt({batch_week_cnt}) |  batch_read_day({batch_read_day})  | ")
        dis = f'MEAN BATCH WD({cluster.wd})::  | TOTAL_READ_DAY  | TOTAL_READ_CNT  |  READ_WEEK  | UNIQUE USER  | READ_CNT(DAY)|'
        print(dis)
        for p in pat:
            seg_pattern = pattern.Pattern(read_day=batch_read_day, week_cnt=batch_week_cnt, type=p)
            seg_res = cluster.mean_batch(batch_combine.filter(seg_pattern)).collect()[0]
            seg_cash = cluster.mean_batch_merge(batch_combine.filter(seg_pattern), batch_buy).collect()[0][-1][-1]
            print(f'TYPE: {p.upper()[:5]}::\t\t| '
                  f'{seg_res[0]} \t\t\t| {seg_res[1]} \t\t\t    | {seg_res[2]} \t\t\t | {seg_res[3]}  \t\t| {seg_res[4]} \t |')
            seg_name = batch_name + f'_{p}Watching'
            cluster.log.update_log({seg_name: seg_res + [seg_cash]})

    print(f"\n{'=' * 10} Batch {i} End {'=' * 10}")
    cluster.log.save(log_save_path)
    cluster.close_cluster()
    print(f'Main Protocol in SPARK END ')


if __name__  == '__main__':
    args = parse_opt()
    pattern_spark_test(args)