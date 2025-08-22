CREATE DATABASE IF NOT EXISTS bookings_db;
USE bookings_db;

-- Bookings table
CREATE TABLE IF NOT EXISTS bookings (
    booking_id VARCHAR(50) PRIMARY KEY,
    citizen_id BIGINT NOT NULL,
    booking_date DATE NOT NULL,
    appointment_date DATE NOT NULL,
    appointment_time TIME NOT NULL,
    check_in_time DATETIME,
    check_out_time DATETIME,
    task_id VARCHAR(50) NOT NULL,
    num_documents INT,
    queue_number INT,
    satisfaction_rating INT
);

-- Staffing table
CREATE TABLE IF NOT EXISTS staffing (
    date DATE PRIMARY KEY,
    section_id VARCHAR(50) NOT NULL,
    employees_on_duty INT NOT NULL,
    total_task_time_minutes DOUBLE NOT NULL
);

-- Tasks table
CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(50) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    section_id VARCHAR(50) NOT NULL,
    section_name VARCHAR(255) NOT NULL
);
