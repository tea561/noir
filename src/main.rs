use std::{
    fs::File,
    i64,
    io::Write,
    time::Duration,
};

use datatypes::vehicle::{VehicleInfo, VehicleIterator};
use noir::{
    operator::Operator,
    prelude::{EventTimeWindow, IteratorSource, ProcessingTimeWindow, StreamOutput},
    EnvironmentConfig, Stream, StreamEnvironment,
};
mod datatypes;

fn max_pos_on_lane(
    source: Stream<VehicleInfo, impl Operator<VehicleInfo> + 'static>,
) -> StreamOutput<Vec<(String, f64)>> {
    let result = 
        source
        .filter(|v| !v.vehicle_lane.is_empty())
        .group_by_avg(|v| v.vehicle_lane.clone(), |v| {
            if let Some(pos) = v.vehicle_pos {
                pos as f64
            } else {
                0.0
            }
        })
        .collect_vec();
    result
        
}

fn crowded_lanes(
    source: Stream<VehicleInfo, impl Operator<VehicleInfo> + 'static>,
) -> StreamOutput<Vec<(String, usize)>> {
    let result: StreamOutput<Vec<(String, usize)>> =
        source
        .filter(|v| !v.vehicle_lane.is_empty())
        .group_by_count(|v| v.vehicle_lane.clone())
        .filter(|res| res.1 > 50)
        .window(ProcessingTimeWindow::sliding(
            Duration::from_secs(30),
            Duration::from_secs(5),
        ))
        .max()
        .collect_vec();

    result
}

fn max_speed_on_lane(
    source: Stream<VehicleInfo, impl Operator<VehicleInfo> + 'static>,
) -> StreamOutput<Vec<(String, VehicleInfo)>> {
    let result = source
        .add_timestamps(
            {
                let mut count = 0;
                move |v| {
                    count += 1;
                    v.timestep_time as i64 + count
                }
            },
            {
                let mut count = 0;
                move |_, &ts| {
                    count += 1;
                    if count % 10 == 0 {
                        Some(ts)
                    } else {
                        None
                    }
                }
            },
        )
        .filter(|v| !v.vehicle_lane.is_empty())
        .group_by(|v| v.vehicle_lane.clone())
        .window(EventTimeWindow::sliding(100000, 50000))
        .max_by_key(|v| {
            if let Some(speed) = v.vehicle_speed {
                speed as i32
            } else {
                0
            }
        })
        .collect_vec();
    result
}

fn save_max_speed_on_lane(res: Vec<(String, VehicleInfo)>) {
    let mut file = File::create("max_speed_on_lane.txt").expect("Failed to create file");

    for (lane, vehicle) in &res {
        let entry = format!("Vehicle lane: {}\nVehicle id: {:?}\n Vehicle speed: {:?}\n\n", lane, vehicle.vehicle_id, vehicle.vehicle_speed);
        file.write_all(entry.as_bytes())
            .expect("Failed to write result to file");
    }
}

fn save_crowded_lanes(res: Vec<(String, usize)>) {
    let mut file = File::create("crowded_lanes.txt").expect("Failed to create file");

    for (lane, vehicle_count) in &res {
        let entry = format!("Vehicle lane: {} Vehicle count: {}\n\n", lane, vehicle_count);
        file.write_all(entry.as_bytes())
            .expect("Failed to write result to file");
    }
}

fn save_max_pos(res: Vec<(String, f64)>) {
    let mut file = File::create("max_position.txt").expect("Failed to create file");
    for (lane, max_pos) in &res {
        let entry = format!("Vehicle lane: {} Max position: {}\n\n", lane, max_pos);
        file.write_all(entry.as_bytes())
            .expect("Failed to write output");
    }
}

#[tokio::main]
async fn main() {
    let file_path = "belgrade_fcd.csv";

    let full_data_iterator = VehicleIterator::new(file_path);

    let (config, _args) = EnvironmentConfig::from_args();
    let mut env = StreamEnvironment::new(config);
    env.spawn_remote_workers();

    let source = IteratorSource::new(full_data_iterator);
    let mut splits = env.stream(source).split(3).into_iter();

    // let result = env
    //     .stream(source)
    //     .group_by_count(|v| v.vehicle_lane.clone())
    //     .window(ProcessingTimeWindow::sliding(Duration::from_secs(2), Duration::from_secs(1)))
    //     .max()
    //     .for_each(|q| println!("{:?}", q));

    // let result = env
    //     .stream(source)
    //     .group_by_count(|x| x.vehicle_lane.clone())
    //     .filter(|x| x.1 > 50)
    //     .for_each(|dumb| println!("{:?}", dumb));

    let max_speed_on_lane_result = max_speed_on_lane(splits.next().unwrap());
    let crowded_lanes_result = crowded_lanes(splits.next().unwrap());
    let steepest_lane_result = max_pos_on_lane(splits.next().unwrap());

    env.execute().await;

    if let Some(res1) = max_speed_on_lane_result.get() {
        save_max_speed_on_lane(res1);
    }

    if let Some(res2) = crowded_lanes_result.get() {
        save_crowded_lanes(res2);
    }

    if let Some(res3) = steepest_lane_result.get() {
        save_max_pos(res3)
    }
}
