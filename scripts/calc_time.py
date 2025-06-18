from pathlib import Path
import sys

def main(log_filepath, nproc):
    log_filepath = Path(log_filepath)

    times_jobtable = []
    times_batchtable = []
    times_calc = []
    with log_filepath.open() as f:
        for line in f.readlines():
            if "time to create batch table" in line:
                times_batchtable.append(float(line.split(' ')[-2]))
            elif "time to create job table" in line:
                times_jobtable.append(float(line.split(' ')[-2]))
            elif "time to perform one aggregation" in line:
                times_calc.append(float(line.split(' ')[-2]))
            elif "time to perform aggregations after job setup" in line:
                total_time = float(line.split(' ')[-1])

    print(f"Number of workers: {nproc}")
    print(f"Total number of partitions used: {len(times_batchtable)}")
    print(f"Total number of aggregate calculations: {len(times_calc)}")
    print(f"Mean Partition Table Creation Time: {sum(times_batchtable)/len(times_batchtable):.02f} seconds")
    print(f"Mean Job Table Creation Time: {sum(times_jobtable)/len(times_jobtable):.02f} seconds")
    print(f"Mean Agg Calc Time: {sum(times_calc)/len(times_calc):.02f} seconds")
    print(f"Total Time: {total_time} seconds")
    print(f"Mean time per calculation (including I/O): {nproc * total_time/len(times_calc):.02f} CPU seconds")

if __name__ == "__main__":
    filepath = sys.argv[1]
    nproc = int(sys.argv[2])
    main(filepath, nproc)