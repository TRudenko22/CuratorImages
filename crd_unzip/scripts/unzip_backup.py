import os
import shutil
import tarfile
from datetime import datetime, timezone
import csv, json
from postgres_interface import (
    update_history_data,
    get_history_data,
    BatchUpdatePostgres,
    postgres_execute,
)

backup_src = os.environ.get("BACKUP_SRC")  # dir of the metrics files
unzip_dir = os.environ.get("UNZIP_DIR")  # dir of the metrics files

if not unzip_dir or not backup_src:
    print("Please set the environment variables BACKUP_SRC and UNZIP_DIR")
    raise SystemExit(1)

elif not os.path.exists(unzip_dir):
    os.makedirs(unzip_dir)


def push_csv_to_db(extracted_csv_path):
    rowcount = 0
    manifest = json.dumps(dict())
    try:
        for bsubdir, _, csvfiles in os.walk(extracted_csv_path):
            for csvf in csvfiles:
                if csvf.endswith(".csv"):
                    csv_full_path = os.path.join(bsubdir, csvf)
                    table_name_local = os.path.splitext(csv_full_path)
                    table_name_local = table_name_local[0][-1]

                    batch_executor = BatchUpdatePostgres()

                    with open(csv_full_path, "r") as f:
                        # Notice that we don't need the `csv` module.
                        next(f)  # Skip the header row.
                        reader = csv.reader(f)

                        for row in reader:
                            report_period_start = row[0].replace(" UTC", "")
                            report_period_end = row[1].replace(" UTC", "")
                            interval_start = row[2].replace(" UTC", "")
                            interval_end = row[3].replace(" UTC", "")

                            if table_name_local == "0":
                                table_name_sql = "logs_0"
                                namespace = row[4]
                                namespace_labels = row[5]
                                if batch_executor.sql_isempty():
                                    batch_executor.set_sql(
                                        """INSERT INTO """
                                        + table_name_sql
                                        + """(report_period_start, report_period_end, interval_start, interval_end, namespace, namespace_labels) VALUES {}"""
                                    )
                                batch_executor.add(
                                    (
                                        report_period_start,
                                        report_period_end,
                                        interval_start,
                                        interval_end,
                                        namespace,
                                        namespace_labels,
                                    )
                                )

                            elif table_name_local == "1":
                                table_name_sql = "logs_1"
                                node = row[4]
                                node_labels = row[5]
                                if batch_executor.sql_isempty():
                                    batch_executor.set_sql(
                                        """INSERT INTO """
                                        + table_name_sql
                                        + """(report_period_start, report_period_end, interval_start, interval_end, node, node_labels) VALUES {}"""
                                    )
                                batch_executor.add(
                                    (
                                        report_period_start,
                                        report_period_end,
                                        interval_start,
                                        interval_end,
                                        node,
                                        node_labels,
                                    )
                                )
                            elif table_name_local == "2":
                                table_name_sql = "logs_2"
                                node = row[4]
                                namespace = row[5]
                                pod = row[6]
                                pod_usage_cpu_core_seconds = (
                                    row[7] if row[7].strip() != "" else None
                                )
                                pod_request_cpu_core_seconds = (
                                    row[8] if row[8].strip() != "" else None
                                )
                                pod_limit_cpu_core_seconds = (
                                    row[9] if row[9].strip() != "" else None
                                )
                                pod_usage_memory_byte_seconds = (
                                    row[10] if row[10].strip() != "" else None
                                )
                                pod_request_memory_byte_seconds = (
                                    row[11] if row[11].strip() != "" else None
                                )
                                pod_limit_memory_byte_seconds = (
                                    row[12] if row[12].strip() != "" else None
                                )
                                node_capacity_cpu_cores = (
                                    row[13] if row[13].strip() != "" else None
                                )
                                node_capacity_cpu_core_seconds = (
                                    row[14] if row[14].strip() != "" else None
                                )
                                node_capacity_memory_bytes = (
                                    row[15] if row[15].strip() != "" else None
                                )
                                node_capacity_memory_byte_seconds = (
                                    row[16] if row[16].strip() != "" else None
                                )
                                resource_id = row[17]
                                pod_labels = row[18]
                                if batch_executor.sql_isempty():
                                    batch_executor.set_sql(
                                        """INSERT INTO """
                                        + table_name_sql
                                        + """(report_period_start, report_period_end, interval_start, interval_end, node, namespace, pod, pod_usage_cpu_core_seconds, pod_request_cpu_core_seconds, pod_limit_cpu_core_seconds,	pod_usage_memory_byte_seconds, pod_request_memory_byte_seconds, pod_limit_memory_byte_seconds, node_capacity_cpu_cores, node_capacity_cpu_core_seconds, node_capacity_memory_bytes, node_capacity_memory_byte_seconds, resource_id, pod_labels) VALUES {}"""
                                    )
                                batch_executor.add(
                                    (
                                        report_period_start,
                                        report_period_end,
                                        interval_start,
                                        interval_end,
                                        node,
                                        namespace,
                                        pod,
                                        pod_usage_cpu_core_seconds,
                                        pod_request_cpu_core_seconds,
                                        pod_limit_cpu_core_seconds,
                                        pod_usage_memory_byte_seconds,
                                        pod_request_memory_byte_seconds,
                                        pod_limit_memory_byte_seconds,
                                        node_capacity_cpu_cores,
                                        node_capacity_cpu_core_seconds,
                                        node_capacity_memory_bytes,
                                        node_capacity_memory_byte_seconds,
                                        resource_id,
                                        pod_labels,
                                    )
                                )
                            elif table_name_local == "3":
                                table_name_sql = "logs_3"
                                namespace = row[4]
                                pod = row[5]
                                persistentvolumeclaim = row[6]
                                persistentvolume = row[7]
                                storageclass = row[8]
                                persistentvolumeclaim_capacity_bytes = (
                                    row[9] if row[9].strip() != "" else None
                                )
                                persistentvolumeclaim_capacity_byte_seconds = (
                                    row[10] if row[10].strip() != "" else None
                                )
                                volume_request_storage_byte_seconds = (
                                    row[11] if row[11].strip() != "" else None
                                )
                                persistentvolumeclaim_usage_byte_seconds = (
                                    row[12] if row[12].strip() != "" else None
                                )
                                persistentvolume_labels = row[13]
                                persistentvolumeclaim_labels = row[14]
                                if batch_executor.sql_isempty():
                                    batch_executor.set_sql(
                                        """INSERT INTO """
                                        + table_name_sql
                                        + """(report_period_start, report_period_end, interval_start, interval_end, namespace, pod, persistentvolumeclaim, persistentvolume, storageclass, persistentvolumeclaim_capacity_bytes, persistentvolumeclaim_capacity_byte_seconds, volume_request_storage_byte_seconds, persistentvolumeclaim_usage_byte_seconds, persistentvolume_labels, persistentvolumeclaim_labels) VALUES {}"""
                                    )
                                batch_executor.add(
                                    (
                                        report_period_start,
                                        report_period_end,
                                        interval_start,
                                        interval_end,
                                        namespace,
                                        pod,
                                        persistentvolumeclaim,
                                        persistentvolume,
                                        storageclass,
                                        persistentvolumeclaim_capacity_bytes,
                                        persistentvolumeclaim_capacity_byte_seconds,
                                        volume_request_storage_byte_seconds,
                                        persistentvolumeclaim_usage_byte_seconds,
                                        persistentvolume_labels,
                                        persistentvolumeclaim_labels,
                                    )
                                )

                        batch_rowcount = batch_executor.clean()
                        if batch_rowcount >= 0 and rowcount >= 0:
                            rowcount += batch_rowcount
                        else:
                            rowcount = -1

                elif csvf.endswith(".json"):
                    csv_full_path = os.path.join(bsubdir, csvf)
                    with open(csv_full_path, "r") as fd:
                        try:
                            manifest = json.dumps(json.load(fd))  # dump into string
                        except Exception as e:
                            print("Fail to read manifest.json for ", bsubdir)
                            print(e)

    except Exception as ex:
        print("An error is occured {0}".format(ex))
    return rowcount, manifest


def gunzip(file_path, output_path, is_push_db=True):
    try:
        file = tarfile.open(file_path)
        file.extractall(output_path)
        file.close()
    # print("Successfully unzipped the file {}".format(file_path))

    except Exception as ex:
        print(
            "Error is occured while extract the file {} and the error is {}".format(
                file_path, ex
            )
        )


if __name__ == "__main__":
    db_newly_unzipped_file_hist = []
    db_unzipped_file_hist = set(get_history_data())  # list lookup? use set
    try:
        for bsubdir, _, bfiles in os.walk(backup_src):
            for bf in bfiles:
                if bf.endswith(".gz"):
                    backup_full_path = os.path.join(bsubdir, bf)
                    file_folder = bf.split(".")[0]
                    unzip_folder_dir = os.path.join(unzip_dir, file_folder)
                    if not os.path.exists(unzip_folder_dir):
                        os.makedirs(unzip_folder_dir)
                    gunzip(backup_full_path, unzip_folder_dir)
                    if not bf in db_unzipped_file_hist:
                        push_rowcnt, manifest = push_csv_to_db(unzip_folder_dir)
                        # update history every time. this prevents data and metadata inconsistency
                        postgres_execute(
                            "INSERT INTO history(file_names, manifest, success, crtime) VALUES {}",
                            [
                                (
                                    bf,
                                    manifest,
                                    push_rowcnt > 0,
                                    datetime.now(timezone.utc),
                                )
                            ],
                        )

                    shutil.rmtree(unzip_folder_dir)

                    # shutil.rmtree(unzip_folder_dir)

    except Exception as ex:
        print(
            "An error is occured while unzip the files into unzip directory {}".format(
                ex
            )
        )
