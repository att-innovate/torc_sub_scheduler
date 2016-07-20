// The MIT License (MIT)
//
// Copyright (c) 2016 AT&T
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

use mesos::proto::TaskID;
use mesos::SchedulerClient;
use std::sync::{Arc, Mutex};


lazy_static! {
    static ref MESOS: Arc<Mutex<Option<SchedulerClient>>> = {
        Arc::new(Mutex::new(None))
    };
}

pub fn set_mesos_client(client_to_set: Option<SchedulerClient>) {
    let mut client = MESOS.lock().unwrap();
    *client = client_to_set;
}

pub fn kill_task(task_name: &String) {
    let mesos = MESOS.lock().unwrap();

    if let Some(ref client) = *mesos {
        let mut task_id = TaskID::new();
        task_id.set_value(task_name.clone());
        match client.kill(task_id, None) {
            Ok(response) => println!("Task Deleted {:?}", response),
            Err(error_msg) => println!("Kill Task Mesos Problem: {}", error_msg),
        }
    } else {
        println!("Error killing task: Mesos-Client not set")
    }
}
