library(ggplot2)
library(data.table)

io_uring <-
  fread("logs/micro-benchmarks/2024_07_24_02_28_00/2024_07_24_02_28_00_single_thread_multiple_conn.csv")
iperf <-
  fread("logs/micro-benchmarks/2024_07_24_02_28_00/2024_07_24_02_46_00_iperf.csv")

io_uring[, model := "io_uring (single-threaded)"]
iperf[, model := "iperf (multi-threaded)"]

dt <-
  rbind(io_uring[traffic == 'ingress', .(connections, `throughput (Gb/s)`, model)], iperf)

ggplot(dt, aes(x = connections, y = `throughput (Gb/s)`, color=model)) +
  geom_point(size=1.4) +
  geom_line(linewidth=0.5, linetype=5) +
  geom_hline(yintercept=50, linetype="dashed",linewidth=1.2, color='#777777') +
  annotate("text", x=3, y=52, label="c6in.8xlarge bandwidth", color='#777777') +
  theme_bw() +
  theme(legend.position = "bottom", legend.title=element_blank(), legend.background = element_rect(fill='transparent')) +
  scale_x_continuous(breaks=1:10) +
  scale_y_continuous(breaks=seq(10,50,10)) +
  guides(colour = guide_legend(nrow = 1))
