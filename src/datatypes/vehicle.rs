use csv::ReaderBuilder;
use serde::{Serialize, Deserialize};

#[derive(Clone, Serialize, Deserialize, Debug)]
pub struct VehicleInfo {
    pub timestep_time: f32,
    pub vehicle_angle: Option<f32>,
    pub vehicle_id: String,
    pub vehicle_lane: String,
    pub vehicle_pos: Option<f32>,
    pub vehicle_slope: Option<f32>,
    pub vehicle_speed: Option<f32>,
    pub vehicle_type: String,
    pub vehicle_x: Option<f32>,
    pub vehicle_y: Option<f32>,
}
pub struct VehicleIterator {
    csv_reader: csv::Reader<std::fs::File>,
}

impl VehicleIterator {
    pub fn new(file_path: &str) -> Self {
        let file = std::fs::File::open(file_path).expect("Failed to open CSV file");
        let csv_reader = ReaderBuilder::new()
            .delimiter(b';')
            .from_reader(file);
        VehicleIterator { csv_reader }
    }
}

impl Iterator for VehicleIterator {
    type Item = VehicleInfo;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(result) = self.csv_reader.deserialize().next() {
            match result {
                Ok(record) => Some(record),
                Err(err) => {
                    eprintln!("Error reading CSV: {:?}", err);
                    None
                }
            }
        } else {
            None
        }
    }
}
