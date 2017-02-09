import datetime

import canal
from influxdb import InfluxDBClient


class IMU(canal.Measurement):
    accelerometer_x = canal.IntegerField()
    accelerometer_y = canal.IntegerField()
    accelerometer_z = canal.IntegerField()
    gyroscope_x = canal.IntegerField()
    gyroscope_y = canal.IntegerField()
    gyroscope_z = canal.IntegerField()
    user_id = canal.Tag()


if __name__ == "__main__":
    start_date = datetime.datetime.now(datetime.timezone.utc)
    duration = datetime.timedelta(seconds=60)
    user_id = 12345678

    client = InfluxDBClient(
        host="localhost",
        port=8086,
        database="canal"
    )

    # Write some dummy IMU data, sampled once per second
    num_imu_samples = int(duration.total_seconds())
    imu = IMU(
        time=[start_date + datetime.timedelta(seconds=d) for d in
              range(num_imu_samples)],
        acc_x=range(0, 1 * num_imu_samples, 1),
        acc_y=range(0, 2 * num_imu_samples, 2),
        acc_z=range(0, 3 * num_imu_samples, 3),
        gyro_x=range(0, 4 * num_imu_samples, 4),
        gyro_y=range(0, 5 * num_imu_samples, 5),
        gyro_z=range(0, 6 * num_imu_samples, 6),
        user_id=user_id
    )
    client.write(
        data=imu.to_line_protocol(),
        params=dict(
            db="canal"
        )
    )

    # Read back the IMU data
    imu_resp = client.query(IMU.make_query_string(
        time__gte=start_date,
        time__lte=start_date + duration,
        user_id=user_id
    ))
    assert imu == IMU.from_json(imu_resp.raw)
