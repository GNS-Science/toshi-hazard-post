# Processing Times
- 4 locations
- 2 IMTs
- 20 agg types
NB: there is a 5s sleep between putting jobs

## FS: AWS, Bucket: CDC-2, NUM_WORKERS: 1
thp_CDC2_AWS_W1.log 

### Data Load Time
```
2024-05-03 18:23:25,518 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 18.34 seconds
2024-05-03 18:23:42,775 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 10.89 seconds
2024-05-03 18:23:58,600 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 10.53 seconds
2024-05-03 18:24:17,779 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 13.71 seconds
2024-05-03 18:24:27,776 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 4.77 seconds
2024-05-03 18:24:36,157 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 2.98 seconds
2024-05-03 18:24:54,052 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 12.67 seconds
2024-05-03 18:25:06,389 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 6.97 seconds
```

### Processing Time
```
2024-05-03 18:23:31,876 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.36 seconds
2024-05-03 18:23:48,066 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.29 seconds
2024-05-03 18:24:04,069 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.47 seconds
2024-05-03 18:24:23,003 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.22 seconds
2024-05-03 18:24:33,100 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.32 seconds
2024-05-03 18:24:41,329 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.17 seconds
2024-05-03 18:24:59,323 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.27 seconds
2024-05-03 18:25:11,530 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.14 seconds
```
### Total Time
```
2024-05-03 18:25:11,554 - toshi_hazard_post.version2.aggregation - INFO - processed 8 calculations in 158.944 seconds
```

## FS: AWS, Bucket: CDC-2, NUM_WORKERS: 8
thp_CDC2_AWS_W8.log

### Data Load Time
```
2024-05-03 18:31:06,726 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 13.72 seconds
2024-05-03 18:31:07,873 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 10.72 seconds
2024-05-03 18:31:54,382 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 39.82 seconds
2024-05-03 18:32:14,950 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 50.17 seconds
2024-05-03 18:32:16,094 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 56.44 seconds
2024-05-03 18:32:20,930 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 28.50 seconds
2024-05-03 18:32:21,007 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 51.11 seconds
2024-05-03 18:32:32,952 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 35.44 seconds
```
### Processing Time
```
2024-05-03 18:31:13,199 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.47 seconds
2024-05-03 18:31:14,133 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.26 seconds
2024-05-03 18:32:00,979 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.60 seconds
2024-05-03 18:32:21,836 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.89 seconds
2024-05-03 18:32:22,653 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.56 seconds
2024-05-03 18:32:27,936 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 7.01 seconds
2024-05-03 18:32:27,982 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.97 seconds
2024-05-03 18:32:38,919 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.97 seconds
```
### Total Time
```
2024-05-03 18:32:39,063 - toshi_hazard_post.version2.aggregation - INFO - processed 8 calculations in 246.501 seconds
```

## FS: LOCAL, BUCKET: CDC-2, NUM_WOKERS: 1
thp_CDC2_LOCAL_W1.log

### Data Load Time
```
2024-05-04 12:17:23,996 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.56 seconds
2024-05-04 12:17:30,763 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.50 seconds
2024-05-04 12:17:36,681 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.55 seconds
2024-05-04 12:17:42,683 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.56 seconds
2024-05-04 12:17:48,233 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.26 seconds
2024-05-04 12:17:53,822 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.26 seconds
2024-05-04 12:17:59,518 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.42 seconds
2024-05-04 12:18:05,264 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.39 seconds
```
### Processing Time
```
2024-05-04 12:17:30,239 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.24 seconds
2024-05-04 12:17:36,130 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.37 seconds
2024-05-04 12:17:42,121 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.44 seconds
2024-05-04 12:17:47,972 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.29 seconds
2024-05-04 12:17:53,555 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.32 seconds
2024-05-04 12:17:59,087 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.27 seconds
2024-05-04 12:18:04,870 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.35 seconds
2024-05-04 12:18:10,520 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 5.26 seconds
```
### Total Time
```
2024-05-04 12:18:10,523 - toshi_hazard_post.version2.aggregation - INFO - processed 8 calculations in 75.161 seconds
```

## FS: LOCAL, BUCKET: CDC-2, NUM_WOKERS: 1
thp_CDC2_LOCAL_W8.log

### Data Load Time
```
2024-05-04 12:21:12,176 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.57 seconds
2024-05-04 12:21:17,197 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.58 seconds
2024-05-04 12:21:22,210 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.59 seconds
2024-05-04 12:21:27,261 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.58 seconds
2024-05-04 12:21:31,926 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.25 seconds
2024-05-04 12:21:36,902 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.26 seconds
2024-05-04 12:21:42,097 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.42 seconds
2024-05-04 12:21:47,111 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 0.45 seconds
```

### Processing Time
```
2024-05-04 12:21:18,800 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.62 seconds
2024-05-04 12:21:23,550 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.35 seconds
2024-05-04 12:21:28,700 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.49 seconds
2024-05-04 12:21:33,566 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.31 seconds
2024-05-04 12:21:38,199 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.27 seconds
2024-05-04 12:21:43,024 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.12 seconds
2024-05-04 12:21:48,340 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.24 seconds
2024-05-04 12:21:53,257 - toshi_hazard_post.version2.aggregation_calc - INFO - time to perform one aggregation after loading data 6.15 seconds
```

### Total Time
```
2024-05-04 12:21:53,348 - toshi_hazard_post.version2.aggregation - INFO - processed 8 calculations in 72.234 seconds
```
Without the sleep(5)
```
2024-05-04 12:26:53,251 - toshi_hazard_post.version2.aggregation - INFO - processed 8 calculations in 47.755 seconds
```
