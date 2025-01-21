import os
import sys
import glob
import logging
import collections
from concurrent.futures import ThreadPoolExecutor
from optparse import OptionParser
import appsinstalled_pb2

from storage_client import StorageManager

BATCH_SIZE = 100_000
NORMAL_ERR_RATE = 0.01
AppsInstalled = collections.namedtuple("AppsInstalled", ["dev_type", "dev_id", "lat", "lon", "apps"])


def dot_rename(path) -> None:
    head, fn = os.path.split(path)
    # atomic in most cases
    os.rename(path, os.path.join(head, "." + fn))


def prepare_data(appsinstalled: AppsInstalled) -> dict[str, str]:
    ua = appsinstalled_pb2.UserApps()
    ua.lat = appsinstalled.lat
    ua.lon = appsinstalled.lon
    ua.apps.extend(appsinstalled.apps)
    return {f"{appsinstalled.dev_type}:{appsinstalled.dev_id}": ua.SerializeToString()}


def insert_appsinstalled(addr: str, data: dict[str, str], thread_executor: ThreadPoolExecutor, dry_run=False) -> bool:
    if dry_run:
        logging.debug(f"Insert {len(data)} data items.")
    else:
        try:
            future = thread_executor.submit(StorageManager.set_many, addr, data)
            future.result(timeout=3)
        except Exception as e:
            logging.exception(f"Cannot write to storage {addr}: {e}")
            return False
        return True


def parse_appsinstalled(line) -> AppsInstalled | None:

    line_parts = line.strip().split("\t")
    if len(line_parts) < 5:
        return

    dev_type, dev_id, lat, lon, raw_apps = line_parts
    if not dev_type or not dev_id:
        return

    try:
        apps = [int(a.strip()) for a in raw_apps.split(",")]
    except ValueError:
        apps = [int(a.strip()) for a in raw_apps.split(",") if a.isidigit()]
        logging.info(f"Not all user apps are digits: `{line}`")

    try:
        lat, lon = float(lat), float(lon)
    except ValueError:
        logging.info(f"Invalid geo coords: `{line}`")
        return

    return AppsInstalled(dev_type, dev_id, lat, lon, apps)


def main(options):
    device_storage = {
        "idfa": options.idfa,
        "gaid": options.gaid,
        "adid": options.adid,
        "dvid": options.dvid,
    }

    with ThreadPoolExecutor() as thread_executor:
        for fn in glob.iglob(options.pattern):
            logging.info(f'Processing {fn}')
            processed = errors = 0
            data = {addr: dict() for addr in device_storage.values()}

            with open(fn) as fd:
                for line in fd:

                    if not (line := line.strip()):
                        continue

                    if not (appsinstalled := parse_appsinstalled(line)):
                        errors += 1
                        continue

                    if not (storage_addr := device_storage.get(appsinstalled.dev_type)):
                        errors += 1
                        logging.error(f"Unknown device type: {appsinstalled.dev_type}")
                        continue

                    data[storage_addr].update(prepare_data(appsinstalled))
                    if (count := len(data[storage_addr])) >= BATCH_SIZE:
                        ok = insert_appsinstalled(storage_addr, data[storage_addr], thread_executor, options.dry)
                        if ok:
                            processed += count
                        else:
                            errors += count

                        data[storage_addr].clear()

            for addr, values_dict in data.items():
                if values_dict:
                    ok = insert_appsinstalled(addr, values_dict, thread_executor, options.dry)
                    if ok:
                        processed += len(values_dict)
                    else:
                        errors += len(values_dict)

            dot_rename(fn)

            err_rate = float(errors) / processed if processed else 1
            if err_rate < NORMAL_ERR_RATE:
                logging.info(f"Acceptable error rate ({err_rate}). Successfull load")
            else:
                logging.error(f"High error rate ({err_rate} > {NORMAL_ERR_RATE}). Failed load")


def prototest():
    sample = "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"
    for line in sample.splitlines():
        dev_type, dev_id, lat, lon, raw_apps = line.strip().split("\t")
        apps = [int(a) for a in raw_apps.split(",") if a.isdigit()]
        lat, lon = float(lat), float(lon)
        ua = appsinstalled_pb2.UserApps()
        ua.lat = lat
        ua.lon = lon
        ua.apps.extend(apps)
        packed = ua.SerializeToString()
        unpacked = appsinstalled_pb2.UserApps()
        unpacked.ParseFromString(packed)
        assert ua == unpacked


if __name__ == '__main__':
    op = OptionParser()
    op.add_option("-t", "--test", action="store_true", default=False)
    op.add_option("-l", "--log", action="store", default=None)
    op.add_option("--dry", action="store_true", default=False)
    op.add_option("--pattern", action="store", default="*.tsv")
    op.add_option("--idfa", action="store", default="127.0.0.1:6380")
    op.add_option("--gaid", action="store", default="127.0.0.1:6381")
    op.add_option("--adid", action="store", default="127.0.0.1:6382")
    op.add_option("--dvid", action="store", default="127.0.0.1:6383")
    (opts, args) = op.parse_args()
    logging.basicConfig(filename=opts.log, level=logging.INFO if not opts.dry else logging.DEBUG,
                        format='[%(asctime)s] %(levelname).1s %(message)s', datefmt='%Y.%m.%d %H:%M:%S')
    if opts.test:
        prototest()
        sys.exit(0)

    logging.info(f"Storage loader started with options: {opts}")
    try:
        main(opts)
    except Exception as e:
        logging.exception(f"Unexpected error: {e}")
        sys.exit(1)
