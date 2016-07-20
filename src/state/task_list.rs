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

use super::state::TaskState;
use std::collections::HashMap;
use std::sync::Mutex;

pub struct TaskList {
    task_list: Mutex<HashMap<String, Task>>,
}

#[derive(Clone, Debug, RustcEncodable)]
pub struct Task {
    pub name: String,
    pub controller: String,
    pub id: String,
    pub image: String,
    pub node_name: String,
    pub node_type: String,
    pub node_function: String,
    pub dependent_service: String,
    pub arguments: String,
    pub parameters: String,
    pub memory: f64,
    pub cpu: f64,
    pub volumes: Vec<Volume>,
    pub privileged: bool,
    pub sla: SLA,
    pub is_metered: bool,
    pub is_system_service: bool,
    pub is_job: bool,
    pub network_type: String,
    pub ip: String,
    pub slave_id: String,
    pub state: TaskState,
    pub last_update: i64,
}

#[derive(Clone, Debug, RustcEncodable)]
pub struct Volume {
    pub host_path: String,
    pub container_path: String,
    pub read_only_mode: bool,
}

#[derive(Clone, Hash, Eq, PartialEq, Debug, RustcEncodable)]
pub enum SLA {
    None,
    SingletonEachNode,
    SingletonEachSlave,
}


impl TaskList {
    pub fn new() -> TaskList {
        TaskList { task_list: Mutex::new(HashMap::new()) }
    }

    pub fn add_new_task(&self, task: &Task) {
        self.task_list.lock().unwrap().insert(task.name.to_string(), task.clone());
    }

    pub fn remove_task_by_name(&self, task_name: String) {
        self.task_list.lock().unwrap().remove(&task_name);
    }

    pub fn set_task_state(&self, task_name: String, task_state: TaskState) {
        match self.task_list.lock().unwrap().get_mut(&task_name) {
            Some(task) => {
                task.state = task_state.clone();
            }
            None => {}
        }
    }

    pub fn set_task_node_name(&self, task_name: String, node_name: String) {
        match self.task_list.lock().unwrap().get_mut(&task_name) {
            Some(task) => {
                task.node_name = node_name.clone();
            }
            None => {}
        }
    }

    pub fn set_task_info(&self, task_name: String, task_id: String, task_ip: String, slave_id: String) {
        match self.task_list.lock().unwrap().get_mut(&task_name) {
            Some(task) => {
                if task_id.len() > 0 {
                    task.id = task_id.clone();
                }
                if task_ip.len() > 0 {
                    task.ip = task_ip.clone();
                }
                if slave_id.len() > 0 {
                    task.slave_id = slave_id.clone();
                }
                println!("task changed {:?}", task);
            }
            None => {}
        }
    }

    pub fn get_task_state(&self, task_name: String) -> TaskState {
        match self.task_list.lock().unwrap().get(&task_name) {
            Some(task) => task.state.clone(),
            None => TaskState::NotRunning,
        }
    }

    pub fn get_task_name_by_id(&self, id_prefix: String) -> String {
        let mut result: String = "".to_string();

        let map = self.task_list.lock().unwrap();
        for value in map.values().into_iter().filter(|value| !value.id.is_empty()) {
            if value.id.starts_with(&id_prefix) {
                result = value.name.clone();
                break;
            }
        }

        result.clone()
    }

    pub fn get_task_ip_by_name(&self, name: String) -> String {
        match self.task_list.lock().unwrap().get(&name) {
            Some(task) => task.ip.clone(),
            None => "".to_string(),
        }
    }

    pub fn get_task(&self, task_name: String) -> Result<Task, &'static str> {
        match self.task_list.lock().unwrap().get(&task_name) {
            Some(task) => Ok(task.clone()),
            None => Err("Can't find task"),
        }
    }

    pub fn get_tasks_with_state(&self, task_state: TaskState) -> Vec<Task> {
        let mut result: Vec<Task> = vec![];

        let map = self.task_list.lock().unwrap();
        for value in map.values().into_iter().filter(|value| value.state == task_state) {
            result.push(value.clone());
        }

        result
    }
}
