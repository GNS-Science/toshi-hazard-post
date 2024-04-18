# Performance

## ARROW micro

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-post$ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2_micro.toml -M ARROW
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation ARROW
=================================================
2024-04-17 21:06:04,776 - toshi_hazard_post.version2.aggregation_arrow - INFO - getting sites . . .
2024-04-17 21:06:04,777 - toshi_hazard_post.version2.aggregation_arrow - INFO - getting logic trees . . .
2024-04-17 21:06:04,779 - toshi_hazard_post.version2.aggregation_arrow - INFO - building hazard logic tree . . .
2024-04-17 21:06:04,779 - toshi_hazard_post.version2.aggregation_arrow - INFO - arrow method
2024-04-17 21:06:04,780 - toshi_hazard_post.version2.aggregation_arrow - INFO - time to build weight table 0.00 seconds
2024-04-17 21:06:04,780 - toshi_hazard_post.version2.aggregation_arrow - DEBUG - (96, 3)
2024-04-17 21:06:04,896 - toshi_hazard_post.version2.data_arrow - INFO - load ds: 0.000616, scanner:0.000213 duck_sql:0.0: to_arrow 0.109467
2024-04-17 21:06:04,898 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to load realizations 0.11 seconds
2024-04-17 21:06:05,315 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to convert_probs_to_rates() 0.42 seconds
2024-04-17 21:06:05,332 - toshi_hazard_post.version2.aggregation_calc_arrow - INFO - RSS: 0MB
2024-04-17 21:06:05,337 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to join_rates_weights() 0.02 seconds
2024-04-17 21:06:05,337 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - rates_weights (96, 4)
2024-04-17 21:06:05,337 - toshi_hazard_post.version2.aggregation_arrow - INFO - time to perform aggregation for one location-imt pair 0.55 seconds
2024-04-17 21:06:05,338 - toshi_hazard_post.version2.aggregation_arrow - INFO - total arrow time: 0.558
```

## ARROW mini

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-post$ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2_mini.toml -M ARROW
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation ARROW
=================================================
2024-04-17 21:08:27,321 - toshi_hazard_post.version2.aggregation_arrow - INFO - time to perform aggregation for one location-imt pair 0.50 seconds
2024-04-17 21:08:27,321 - toshi_hazard_post.version2.aggregation_arrow - INFO - total arrow time: 0.747
```

## ARROW NSHM

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-post$ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml -M ARROW
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation ARROW
=================================================
2024-04-17 21:09:39,864 - toshi_hazard_post.version2.aggregation_arrow - INFO - time to build weight table 18.28 seconds
2024-04-17 21:09:39,864 - toshi_hazard_post.version2.aggregation_arrow - DEBUG - (3919104, 3)
2024-04-17 21:09:40,010 - toshi_hazard_post.version2.aggregation_arrow - INFO - RSS: 149MB
2024-04-17 21:09:40,155 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to load realizations 0.14 seconds
2024-04-17 21:09:40,155 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - rlz_table (912, 3)
2024-04-17 21:09:40,547 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to convert_probs_to_rates() 0.39 seconds
2024-04-17 21:09:41,970 - toshi_hazard_post.version2.aggregation_calc_arrow - INFO - rates_weights_joined shape: (3919104, 4)
2024-04-17 21:09:43,855 - toshi_hazard_post.version2.aggregation_calc_arrow - INFO - RSS: 149MB
2024-04-17 21:09:43,866 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - time to join_rates_weights() 3.32 seconds
2024-04-17 21:09:43,866 - toshi_hazard_post.version2.aggregation_calc_arrow - DEBUG - rates_weights (3919104, 4)
2024-04-17 21:09:43,928 - toshi_hazard_post.version2.aggregation_arrow - INFO - time to perform aggregation for one location-imt pair 3.92 seconds
2024-04-17 21:09:43,928 - toshi_hazard_post.version2.aggregation_arrow - INFO - total arrow time: 22.343
```

## Original NSHM

```
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-post$ poetry run python ./scripts/thp_v2.py aggregate demo/hazard_v2.toml -M OG
warning openquake module dependency not available, maybe you want to install
                with nzshm-model[openquake]
Toshi Hazard Post: hazard curve aggregation OG
==============================================
2024-04-17 21:10:52,479 - toshi_hazard_post.version2.aggregation - INFO - time to calculate weights 9.14 seconds
979776
2024-04-17 21:10:56,043 - toshi_hazard_post.version2.data - INFO - loaded 912 realizations and 912 entries
2024-04-17 21:10:56,043 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to load realizations 3.56 seconds
2024-04-17 21:11:21,688 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to build branch rates 25.65 seconds
2024-04-17 21:11:21,688 - toshi_hazard_post.version2.aggregation_calc - DEBUG - branch_rates with shape (979776, 44)
2024-04-17 21:11:21,688 - toshi_hazard_post.version2.aggregation_calc - DEBUG - weights with shape (979776,)
2024-04-17 21:11:26,251 - toshi_hazard_post.version2.aggregation_calc - DEBUG - agg with shape (44, 6)
2024-04-17 21:11:26,251 - toshi_hazard_post.version2.aggregation_calc - DEBUG - time to calculate aggs 4.56 seconds
2024-04-17 21:11:26,251 - toshi_hazard_post.version2.aggregation_calc - INFO - saving result . . .
2024-04-17 21:11:26,389 - toshi_hazard_post.version2.aggregation - INFO - time to perform aggregation for one location-imt pair 33.91 seconds
2024-04-17 21:11:26,390 - toshi_hazard_post.version2.aggregation - INFO - total OG time: 43.052995
chrisbc@tryharder-ubuntu:/GNSDATA/LIB/toshi-hazard-post$
```

