# Batch Adaptive Clients Segmentation on kakao webtoon using spark

> `CSI TERM PROJECT` `HAIL` `22.06.18 ~ 22.06.22`
> 
> 

## 1. batch adaptive
- for big-data which can't load on mem, use spark with batch adaptive way
- there are some variation between batch, i would do adaptive to target period
- for newbie which don't have any data, make inference more easy

## 2. on user service use pattern
- based on active signal for user, marketing or targeting should be there 
- i'd focused on to explain user's pattern
- 
------------
## Architecture
![arc](img/arc.png?raw=true 'arc')

------------------

## RESULT

### segment result on traffic
![res_2](img/total_cnt.png?raw=true 'res_2') ![res_3](img/total_read_day.png?raw=true 'res_3')

## RUN 
### currently serve only default option, num_worker =3 , root path on cur_path 
```shell
python deploy.py
```

