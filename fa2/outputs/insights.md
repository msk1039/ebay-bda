### Key findings

1. **Q1 - Growth was steep and Black Friday was the biggest spike.** Monthly orders rose from 787 (Jan 2017) to 6,421 (Aug 2018), about 8.16x. November 2017 peaked at 7,421 orders (BRL 1,003,862), up 63.2% on October, with a single-day peak on 2017-11-24 (1,166 orders), then fell 24.3% in December. *So what:* a marketplace must provision capacity and promotions for a few extreme days, not the average month. Black Friday is a plausible cause, not proven.
2. **Q2 - Buyers shop on weekdays, in the afternoon and evening.** Monday is busiest (16.3% of orders) and Saturday quietest (11.0%); 14:00-17:00 alone holds 19.8% of orders and the single busiest slot is Tuesday 14:00 (1,106 orders). *So what:* send campaigns and push notifications just before these windows, and schedule heavy batch jobs overnight (00:00-06:00 is only 4.8% of orders).
3. **Q3 - Demand is concentrated in a few states.** SP, RJ and MG together give 63.35% of GMV; SP alone has 41.88% of orders and the city of Sao Paulo 15.57%. Buyers in RJ spend more per customer (BRL 148.00) than SP (BRL 129.92). *So what:* growth is mostly about deepening the south-east, while the remaining states are a smaller, harder-to-serve opportunity.
4. **Q4 - Late delivery goes with much worse reviews.** On-time orders average 4.29 stars versus 2.27 for late ones, and 53.77% of late orders get 1 star versus 6.62% on time. The national late rate is 6.77% but MA reaches 17.43% (avg 21.5 days) while SP is 4.49%. *So what:* delivery promises directly shape seller ratings and repeat trust. This is correlation: late orders may also differ in other ways (product, seller), and the overall delay-versus-score correlation is only -0.27.
5. **Q5 - SP is the supply hub, so most orders cross state lines.** 64.18% of order flows are inter-state and only 35.82% stay in-state; SP sellers originate 70.76% of all flows. Average freight is BRL 23.69 inter-state versus BRL 13.45 intra-state, and SP to BA takes 19.68 days versus 7.86 for SP to SP. *So what:* distance costs buyers money and time, so geographic seller mix matters. Multi-seller orders are counted once per state pair.
6. **Q6 - A few categories earn most of the money.** The top 10 of 74 categories earn 62.37% of GMV; health_beauty leads (9.31%), then watches_gifts (8.88%). Review scores differ (e.g. bed_bath_table 3.90, cool_stuff 4.16). *So what:* focus merchandising on the top 10 but monitor quality, since big categories are not automatically the best-rated.

### Connecting the findings

- **Distance chain:** 59.74% of sellers (1,849 of 3,095) are in SP (Q3), so most orders ship from SP (Q5). Far-away buyers wait longer (BA 19.3 days, MA 21.5, CE 21.2) and are late more often (MA 17.43%, CE 13.76%, BA 12.16%) (Q4), and late orders score about 2 stars lower.
- **Supply gap:** BA has only 19 sellers for 3,344 orders, so almost everything must be shipped in from elsewhere (Q3, Q5).
- **Peak load meets fulfilment:** the November 2017 surge (Q1) hit a network where inter-state shipping is already slow (Q5). Whether the surge itself raised lateness is not tested here.

### Recommendations

| Recommendation | Evidence (numbers) | Who acts | Expected impact |
|---|---|---|---|
| Plan capacity, stock and promotions before Black Friday | Nov 2017: 7,421 orders, +63.2% MoM, then -24.3% in Dec | Marketplace, sellers | Fewer stock-outs and slowdowns at peak; smoother December |
| Time campaigns and notifications to 14:00-17:00 on weekdays and Sunday evenings | 14:00-17:00 = 19.8% of orders; Mon 16.3% vs Sat 11.0% | Marketplace | Better response per message; batch jobs moved to quiet hours |
| Recruit sellers or open fulfilment hubs outside SP, starting with BA, CE, MA and RJ | SP sellers = 70.76% of flows; inter-state freight BRL 23.69 vs 13.45 | Marketplace, logistics | Shorter shipping, lower freight, fewer late orders |
| Show realistic delivery estimates and send proactive delay messages in high-late-rate states | MA 17.43%, CE 13.76%, BA 12.16% late vs SP 4.49% | Marketplace, logistics | Fewer unpleasant surprises, so fewer 1-star reviews |
| Track on-time delivery as a seller-quality metric | Late orders: 2.27 stars vs 4.29; 1-star 53.77% vs 6.62% | Marketplace, sellers | Ratings better reflect seller service; early warning on weak carriers |
| Prioritise the top-10 categories but watch their review scores | Top 10 = 62.37% of GMV; bed_bath_table 3.90 stars | Marketplace, sellers | Revenue focus without letting quality slip in large categories |

### How this maps to eBay's Big Data stack (FA1 link)

- **Storage and compute:** at eBay scale the same PySpark code would run on a YARN cluster reading from HDFS instead of local CSVs. Only the paths and the cluster size change.
- **Parquet:** columnar files let Spark read only the needed columns, which suits wide event tables far better than CSV.
- **Streaming:** the late-delivery logic (promised date versus now) could run in Spark Structured Streaming to raise real-time delay alerts.
- **Serving:** results such as per-state late rates or seller scores could be pushed to HBase or Cassandra for low-latency lookups on the website.

### Limitations

- Olist is a stand-in: Brazilian data from 2016-2018, so patterns (SP hub, Black Friday) will not transfer exactly to eBay.
- All links shown are correlations, not proven causes.
- Thin months (2016-09, 2016-12, 2018-09, 2018-10) were excluded from the Q1 trend.
- Q5 counts a multi-seller order once per seller-state and buyer-state pair, and delivery metrics cover delivered orders only.
