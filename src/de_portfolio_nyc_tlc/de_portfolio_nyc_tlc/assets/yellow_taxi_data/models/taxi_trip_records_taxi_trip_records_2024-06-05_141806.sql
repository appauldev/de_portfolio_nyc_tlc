/*!40101 SET NAMES utf8 */;
/*!40014 SET FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET SQL_NOTES=0 */;
DROP TABLE IF EXISTS taxi_trip_records;
CREATE TABLE taxi_trip_records(trip_id BIGINT DEFAULT(nextval('pk_seq')) PRIMARY KEY, vendor_id TINYINT, pickup_dtime TIMESTAMP, dropoff_dtime TIMESTAMP, passenger_count TINYINT, trip_distance DOUBLE, rate_code_id TINYINT, store_and_fwd_flag TINYINT, pickup_lid SMALLINT, dropoff_lid SMALLINT, payment_type TINYINT, fare_amount DOUBLE, extra DOUBLE, mta_tax DOUBLE, tip_amount DOUBLE, tolls_amount DOUBLE, improvement_surcharge DOUBLE, total_amount DOUBLE, congestion_surcharge DOUBLE, airport_fee DOUBLE, __index_level_0__ BIGINT);