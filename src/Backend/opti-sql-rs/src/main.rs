use datafusion::arrow::record_batch::RecordBatch;
mod project;
fn main(){
    println!("Hello, world!");
    project::project_exec::project_execute();
    project::source::csv::read_csv();
    
}