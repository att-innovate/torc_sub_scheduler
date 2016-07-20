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

use std::thread;
use std::fs::File;
use std::io::Read;
use std::sync::mpsc::{Receiver, Sender, channel};
use std::time::Duration;
use yaml_rust::{Yaml, YamlLoader};
use collaborator::{kill_task, register_running_task};
use utils::{read_int, read_string};
use super::task_list::{SLA, Task, TaskList, Volume};
use super::node_list::{Node, NodeList};
use uuid::Uuid;
use chrono::UTC;

#[derive (Clone)]
pub struct StateManager {
    sender: Sender<StateRequestMsg>,
    master_ip: String,
    my_name: String,
    controller_ip: String,
    my_framework_id: String,
    config: Yaml,
}

#[derive(Clone, Hash, Eq, PartialEq, Debug, RustcEncodable)]
pub enum TaskState {
    NotRunning,
    Requested,
    Accepted,
    Running,
}


impl StateManager {
    pub fn new(master_ip: String, controller_ip: String, config_file: String) -> StateManager {
        let (tx, rx) = channel();
        let config = StateManager::read_config_file(config_file);
        let my_name = config["name"].as_str().unwrap_or("torc-controller").to_string();
        let statemanager = StateManager {
            sender: tx,
            master_ip: master_ip,
            my_name: my_name.clone(),
            controller_ip: controller_ip,
            my_framework_id: format!("{}-{}", my_name.clone(), Uuid::new_v4().to_simple_string()),
            config: config,
        };
        statemanager.start_serving(rx);
        statemanager.load_node_list();
        statemanager.start_syncing();
        statemanager
    }

    pub fn get_master_ip(&self) -> String {
        self.master_ip.clone()
    }

    pub fn get_my_name(&self) -> String {
        self.my_name.clone()
    }

    pub fn get_my_framework_id(&self) -> String {
        self.my_framework_id.clone()
    }

    pub fn get_controller_ip(&self) -> String {
        self.controller_ip.clone()
    }

    pub fn get_yaml(&self) -> Yaml {
        self.config.clone()
    }

    pub fn send_ping(&self) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::Ping { sender: sender };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn request_task_state(&self, task_name: String) -> TaskState {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetTaskState {
            sender: sender,
            task_name: task_name,
        };
        self.sender.send(msg).unwrap();

        let state = match receiver.recv().unwrap() {
            StateResponseMsg::TasksState { tasks_state } => tasks_state,
            _ => TaskState::NotRunning,
        };

        state
    }

    pub fn request_task_name_by_id(&self, id_prefix: String) -> String {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetTaskNameById {
            sender: sender,
            id_prefix: id_prefix,
        };
        self.sender.send(msg).unwrap();

        let task_name: String = match receiver.recv().unwrap() {
            StateResponseMsg::TaskName { task_name } => task_name,
            _ => "".to_string(),
        };

        task_name.clone()
    }

    pub fn request_task_ip_by_name(&self, name: String) -> String {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetTaskIPByName {
            sender: sender,
            name: name,
        };
        self.sender.send(msg).unwrap();

        let task_ip: String = match receiver.recv().unwrap() {
            StateResponseMsg::TaskIP { task_ip } => task_ip,
            _ => "".to_string(),
        };

        task_ip.clone()
    }

    pub fn send_update_task_state(&self, task_name: String, task_state: TaskState) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::UpdateTaskState {
            sender: sender,
            task_name: task_name,
            task_state: task_state,
        };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn send_update_task_node_name(&self, task_name: String, node_name: String) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::UpdateTaskNodeName {
            sender: sender,
            task_name: task_name,
            node_name: node_name,
        };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn send_update_task_info(&self, task_name: String, id: String, ip: String, slave_id: String) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::UpdateTaskInfo {
            sender: sender,
            task_name: task_name,
            id: id,
            ip: ip,
            slave_id: slave_id,
        };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn send_start_task(&self,
                           name: &String,
                           image: &String,
                           node_name: &String,
                           node_type: &String,
                           node_function: &String,
                           dependent_service: &String,
                           arguments: &String,
                           parameters: &String,
                           memory: &f64,
                           cpu: &f64,
                           volumes: &Vec<Volume>,
                           privileged: &bool,
                           sla: &SLA,
                           is_metered: &bool,
                           is_system_service: &bool,
                           is_job: &bool,
                           network_type: &String) {

        let (sender, receiver) = channel();

        let resolved_arguments = self.resolve_arguments(&arguments);

        let new_task = Task {
            name: name.clone(),
            controller: self.get_my_name(),
            id: "".to_string(),
            image: image.clone(),
            node_name: node_name.clone(),
            node_type: node_type.clone(),
            node_function: node_function.clone(),
            dependent_service: dependent_service.clone(),
            arguments: resolved_arguments.clone(),
            parameters: parameters.clone(),
            memory: memory.clone(),
            cpu: cpu.clone(),
            privileged: privileged.clone(),
            sla: sla.clone(),
            is_metered: is_metered.clone(),
            is_system_service: is_system_service.clone(),
            is_job: is_job.clone(),
            volumes: volumes.clone(),
            network_type: network_type.clone(),
            ip: "".to_string(),
            slave_id: "".to_string(),
            state: TaskState::Requested,
            last_update: UTC::now().timestamp(),
        };

        let msg = StateRequestMsg::StartTask {
            sender: sender,
            task: new_task,
        };

        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn send_kill_task_by_name(&self, task_name: String) {
        kill_task(&task_name);
    }

    pub fn send_remove_task_by_name(&self, task_name: String) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::RemoveTask {
            sender: sender,
            task_name: task_name,
        };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn request_list_requested_tasks(&self) -> Vec<Task> {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetRequestedTasks { sender: sender };
        self.sender.send(msg).unwrap();

        let result: Vec<Task> = match receiver.recv().unwrap() {
            StateResponseMsg::GetRequestedTasks { requested_tasks } => requested_tasks,
            _ => vec![],
        };

        result
    }

    pub fn request_list_running_tasks(&self) -> Vec<Task> {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetRunningTasks { sender: sender };
        self.sender.send(msg).unwrap();

        let result: Vec<Task> = match receiver.recv().unwrap() {
            StateResponseMsg::GetRunningTasks { running_tasks } => running_tasks,
            _ => vec![],
        };

        result
    }

    pub fn send_add_node(&self,
                         name: String,
                         ip: String,
                         external_ip: String,
                         management_ip: String,
                         port_id: i64,
                         node_type: String) {
        let (sender, receiver) = channel();

        let new_node = Node {
            name: name.clone(),
            ip: ip.clone(),
            external_ip: external_ip.clone(),
            management_ip: management_ip.clone(),
            node_type: node_type.clone(),
            node_function: "none".to_string(),
            active: false,
            slave_id: "".to_string(),
            port_id: port_id,
            reachable: false,
        };

        let msg = StateRequestMsg::AddNode {
            sender: sender,
            node: new_node,
        };

        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn request_is_node_active(&self, node_name: String) -> bool {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetIsNodeActive {
            sender: sender,
            node_name: node_name,
        };
        self.sender.send(msg).unwrap();

        let is_active = match receiver.recv().unwrap() {
            StateResponseMsg::GetIsNodeActive { is_active } => is_active,
            _ => false,
        };

        is_active
    }

    pub fn send_update_node(&self, node_name: String, node_type: String, node_function: String, slave_id: String) {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::UpdateNode {
            sender: sender,
            node_name: node_name,
            node_type: node_type,
            node_function: node_function,
            slave_id: slave_id,
        };
        self.sender.send(msg).unwrap();
        receiver.recv().unwrap();
    }

    pub fn request_node(&self, node_name: String) -> Option<Node> {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetNode {
            sender: sender,
            node_name: node_name,
        };
        self.sender.send(msg).unwrap();

        let result = match receiver.recv().unwrap() {
            StateResponseMsg::GetNode { node } => Some(node),
            _ => None,
        };

        result
    }

    pub fn request_list_nodes(&self) -> Vec<Node> {
        let (sender, receiver) = channel();

        let msg = StateRequestMsg::GetNodes { sender: sender };
        self.sender.send(msg).unwrap();

        let result: Vec<Node> = match receiver.recv().unwrap() {
            StateResponseMsg::GetNodes { nodes } => nodes,
            _ => vec![],
        };

        result
    }
}

struct State {
    initialized: bool,
    controller_ip: String,
    task_list: TaskList,
    node_list: NodeList,
}

enum StateRequestMsg {
    Ping {
        sender: Sender<StateResponseMsg>,
    },
    GetTaskState {
        sender: Sender<StateResponseMsg>,
        task_name: String,
    },
    GetTaskNameById {
        sender: Sender<StateResponseMsg>,
        id_prefix: String,
    },
    GetTaskIPByName {
        sender: Sender<StateResponseMsg>,
        name: String,
    },
    UpdateTaskState {
        sender: Sender<StateResponseMsg>,
        task_name: String,
        task_state: TaskState,
    },
    UpdateTaskNodeName {
        sender: Sender<StateResponseMsg>,
        task_name: String,
        node_name: String,
    },
    UpdateTaskInfo {
        sender: Sender<StateResponseMsg>,
        task_name: String,
        id: String,
        ip: String,
        slave_id: String,
    },
    StartTask {
        sender: Sender<StateResponseMsg>,
        task: Task,
    },
    RemoveTask {
        sender: Sender<StateResponseMsg>,
        task_name: String,
    },
    GetRequestedTasks {
        sender: Sender<StateResponseMsg>,
    },
    GetRunningTasks {
        sender: Sender<StateResponseMsg>,
    },
    AddNode {
        sender: Sender<StateResponseMsg>,
        node: Node,
    },
    GetIsNodeActive {
        sender: Sender<StateResponseMsg>,
        node_name: String,
    },
    UpdateNode {
        sender: Sender<StateResponseMsg>,
        node_name: String,
        node_type: String,
        node_function: String,
        slave_id: String,
    },
    GetNode {
        sender: Sender<StateResponseMsg>,
        node_name: String,
    },
    GetNodes {
        sender: Sender<StateResponseMsg>,
    },
}

enum StateResponseMsg {
    Pong,
    TasksState {
        tasks_state: TaskState,
    },
    TaskName {
        task_name: String,
    },
    TaskIP {
        task_ip: String,
    },
    UpdateTaskState,
    UpdateTaskInfo,
    UpdateTaskNodeName,
    StartTask,
    RemoveTask,
    GetRequestedTasks {
        requested_tasks: Vec<Task>,
    },
    GetRunningTasks {
        running_tasks: Vec<Task>,
    },
    AddNode,
    GetIsNodeActive {
        is_active: bool,
    },
    UpdateNode,
    GetNodes {
        nodes: Vec<Node>,
    },
    GetNode {
        node: Node,
    },
}


impl StateManager {
    fn read_config_file(config_file: String) -> Yaml {
        let mut file = match File::open(config_file) {
            Ok(file) => file,
            Err(err) => panic!(err.to_string()),
        };

        let mut content = String::new();
        file.read_to_string(&mut content).unwrap();
        let config = YamlLoader::load_from_str(&content).unwrap();
        // Multi document support, doc is a yaml::Yaml
        config[0].clone()
    }

    fn start_serving(&self, rx: Receiver<StateRequestMsg>) {
        let controller_ip = self.controller_ip.clone();
        thread::Builder::new()
            .name("state-serve".to_string())
            .spawn(move || {
                let mut state = State {
                    initialized: false,
                    controller_ip: controller_ip,
                    task_list: TaskList::new(),
                    node_list: NodeList::new(),
                };
                state.initialized = true;

                loop {
                    match rx.recv().unwrap() {
                        StateRequestMsg::Ping { sender } => StateManager::ping(sender),
                        StateRequestMsg::GetTaskState { sender, task_name } => {
                            StateManager::get_task_state(sender, &state, task_name)
                        }
                        StateRequestMsg::GetTaskNameById { sender, id_prefix } => {
                            StateManager::get_task_name_by_id(sender, &state, id_prefix)
                        }
                        StateRequestMsg::GetTaskIPByName { sender, name } => {
                            StateManager::get_task_ip_by_name(sender, &state, name)
                        }
                        StateRequestMsg::UpdateTaskState { sender, task_name, task_state } => {
                            StateManager::update_task_state(sender, &state, task_name, task_state)
                        }
                        StateRequestMsg::UpdateTaskNodeName { sender, task_name, node_name } => {
                            StateManager::update_task_node_name(sender, &state, task_name, node_name)
                        }
                        StateRequestMsg::UpdateTaskInfo { sender, task_name, id, ip, slave_id } => {
                            StateManager::update_task_info(sender, &state, task_name, id, ip, slave_id)
                        }
                        StateRequestMsg::StartTask { sender, task } => StateManager::start_task(sender, &state, &task),
                        StateRequestMsg::RemoveTask { sender, task_name } => {
                            StateManager::remove_task_by_name(sender, &state, task_name)
                        }
                        StateRequestMsg::GetRequestedTasks { sender } => StateManager::get_requested_tasks(sender, &state),
                        StateRequestMsg::GetRunningTasks { sender } => StateManager::get_running_tasks(sender, &state),
                        StateRequestMsg::AddNode { sender, node } => StateManager::add_node(sender, &state, &node),
                        StateRequestMsg::GetIsNodeActive { sender, node_name } => {
                            StateManager::get_is_node_active(sender, &state, node_name)
                        }
                        StateRequestMsg::UpdateNode { sender, node_name, node_type, node_function, slave_id } => {
                            StateManager::update_node(sender,
                                                      &state,
                                                      node_name,
                                                      node_type,
                                                      node_function,
                                                      slave_id)
                        }
                        StateRequestMsg::GetNode { sender, node_name } => StateManager::get_node(sender, &state, node_name),
                        StateRequestMsg::GetNodes { sender } => StateManager::get_nodes(sender, &state),
                    }
                }
            })
            .unwrap();
    }

    fn start_syncing(&self) {
        let config = self.get_yaml();
        let wait_time = config["statesync"]["poll_interval_in_seconds"].as_i64().unwrap() as u64;
        let state_manager = self.clone();

        thread::Builder::new()
            .name("state-sync".to_string())
            .spawn(move || {
                loop {
                    thread::sleep(Duration::from_secs(wait_time));
                    println!("syncing ....");
                    let running_tasks = state_manager.request_list_running_tasks();
                    for task in &running_tasks {
                        register_running_task(&state_manager.get_controller_ip(), &task)
                    }
                }
            })
            .unwrap();
    }

    fn load_node_list(&self) {
        let config = self.get_yaml();
        let nodes = config["nodes"].as_vec().unwrap();
        for node in nodes {
            self.send_add_node(read_string(node, "name".to_string()),
                               read_string(node, "ip".to_string()),
                               read_string(node, "external_ip".to_string()),
                               read_string(node, "management_ip".to_string()),
                               read_int(node, "port".to_string(), 0),
                               read_string(node, "type".to_string()))
        }
    }

    fn ping(sender: Sender<StateResponseMsg>) {
        println!("got ping");
        let msg = StateResponseMsg::Pong;
        sender.send(msg).unwrap();
    }

    fn get_task_state(sender: Sender<StateResponseMsg>, state: &State, task_name: String) {
        let task_state = state.task_list.get_task_state(task_name);
        let msg = StateResponseMsg::TasksState { tasks_state: task_state };
        sender.send(msg).unwrap();
    }

    fn get_task_name_by_id(sender: Sender<StateResponseMsg>, state: &State, id_prefix: String) {
        let task_name = state.task_list.get_task_name_by_id(id_prefix);
        let msg = StateResponseMsg::TaskName { task_name: task_name };
        sender.send(msg).unwrap();
    }

    fn get_task_ip_by_name(sender: Sender<StateResponseMsg>, state: &State, name: String) {
        let task_ip = state.task_list.get_task_ip_by_name(name);
        let msg = StateResponseMsg::TaskIP { task_ip: task_ip };
        sender.send(msg).unwrap();
    }

    fn update_task_state(sender: Sender<StateResponseMsg>, state: &State, task_name: String, task_state: TaskState) {
        state.task_list.set_task_state(task_name.to_string(), task_state.clone());

        match task_state {
            TaskState::Running => {
                let result = state.task_list.get_task(task_name.clone());
                match result {
                    Ok(task) => register_running_task(&state.controller_ip.clone(), &task),
                    Err(error_msg) => {
                        println!("error [{:?}] while retrieving {}",
                                 error_msg,
                                 task_name.clone())
                    }
                }

            }
            _ => {}                
        }

        let msg = StateResponseMsg::UpdateTaskState;
        sender.send(msg).unwrap();
    }

    fn update_task_node_name(sender: Sender<StateResponseMsg>, state: &State, task_name: String, node_name: String) {
        state.task_list.set_task_node_name(task_name.to_string(), node_name);

        let msg = StateResponseMsg::UpdateTaskNodeName;
        sender.send(msg).unwrap();
    }

    fn update_task_info(sender: Sender<StateResponseMsg>,
                        state: &State,
                        task_name: String,
                        id: String,
                        ip: String,
                        slave_id: String) {
        state.task_list.set_task_info(task_name.to_string(), id, ip, slave_id);

        let msg = StateResponseMsg::UpdateTaskInfo;
        sender.send(msg).unwrap();
    }

    fn start_task(sender: Sender<StateResponseMsg>, state: &State, task: &Task) {
        println!("start task {}", task.name);

        state.task_list.add_new_task(&task);
        let msg = StateResponseMsg::StartTask;
        sender.send(msg).unwrap();
    }

    fn remove_task_by_name(sender: Sender<StateResponseMsg>, state: &State, task_name: String) {
        println!("remove task {}", task_name);

        state.task_list.remove_task_by_name(task_name.to_string());
        let msg = StateResponseMsg::RemoveTask;
        sender.send(msg).unwrap();
    }

    fn get_requested_tasks(sender: Sender<StateResponseMsg>, state: &State) {
        let result: Vec<Task> = state.task_list.get_tasks_with_state(TaskState::Requested);
        let msg = StateResponseMsg::GetRequestedTasks { requested_tasks: result };
        sender.send(msg).unwrap();
    }

    fn get_running_tasks(sender: Sender<StateResponseMsg>, state: &State) {
        let result: Vec<Task> = state.task_list.get_tasks_with_state(TaskState::Running);
        let msg = StateResponseMsg::GetRunningTasks { running_tasks: result };
        sender.send(msg).unwrap();
    }

    fn resolve_arguments(&self, argument: &String) -> String {
        if argument.len() == 0 {
            return argument.clone();
        }

        let mut resolved_arguments = str::replace(&argument, "$MASTER_IP", &self.master_ip);

        resolved_arguments = match resolved_arguments.contains("$IP_DNS_SL1") {
            true => {
                resolved_arguments.replace("$IP_DNS_SL1",
                             &self.request_task_ip_by_name("dns-sl1".to_string()))
                    .clone()
            }
            false => resolved_arguments.clone(),
        };
        println!("arguments: {}", resolved_arguments);
        resolved_arguments.clone()
    }

    fn add_node(sender: Sender<StateResponseMsg>, state: &State, node: &Node) {
        state.node_list.add_new_node(&node);
        let msg = StateResponseMsg::AddNode;
        sender.send(msg).unwrap();
    }

    fn get_is_node_active(sender: Sender<StateResponseMsg>, state: &State, node_name: String) {
        let is_active = state.node_list.is_node_active(node_name.clone());
        let msg = StateResponseMsg::GetIsNodeActive { is_active: is_active };
        sender.send(msg).unwrap();
    }

    fn update_node(sender: Sender<StateResponseMsg>,
                   state: &State,
                   node_name: String,
                   node_type: String,
                   node_function: String,
                   slave_id: String) {
        state.node_list.update_node(node_name.clone(),
                                    node_type.clone(),
                                    node_function.clone(),
                                    slave_id.clone());
        let msg = StateResponseMsg::UpdateNode;
        sender.send(msg).unwrap();
    }

    fn get_node(sender: Sender<StateResponseMsg>, state: &State, node_name: String) {
        let result: Node = state.node_list.get_node(node_name.clone()).unwrap();
        let msg = StateResponseMsg::GetNode { node: result };
        sender.send(msg).unwrap();
    }

    fn get_nodes(sender: Sender<StateResponseMsg>, state: &State) {
        let result: Vec<Node> = state.node_list.get_nodes();
        let msg = StateResponseMsg::GetNodes { nodes: result };
        sender.send(msg).unwrap();
    }
}
