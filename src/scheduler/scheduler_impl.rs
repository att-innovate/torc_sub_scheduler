#![allow(unused_variables)]

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

use mesos::{Scheduler, SchedulerClient};
use mesos::proto::{CommandInfo, ContainerInfo, ContainerInfo_DockerInfo, ContainerInfo_DockerInfo_Network, ContainerInfo_Type,
                   ExecutorID, FrameworkID, InverseOffer, Offer, OfferID, Parameter, SlaveID, TaskInfo, TaskStatus, Volume,
                   Volume_Mode};
use protobuf;
use mesos::proto::TaskState as MesosTaskState;
use mesos::util;
use state::{StateManager, TaskState};
use utils;
use collaborator::set_mesos_client;


pub struct TorcScheduler<'lifetime> {
    pub state_manager: &'lifetime StateManager,
}

impl<'lifetime> Scheduler for TorcScheduler<'lifetime> {
    fn subscribed(&mut self, client: &SchedulerClient, framework_id: &FrameworkID, heartbeat_interval_seconds: Option<f64>) {
        println!("received subscribed with id: {}", framework_id.get_value());

        set_mesos_client(Some(client.clone()));

        client.reconcile(vec![]).unwrap();
    }

    fn inverse_offers(&mut self, client: &SchedulerClient, inverse_offers: Vec<&InverseOffer>) {
        println!("received inverse offers");

        // this never lets go willingly
        let offer_ids = inverse_offers.iter()
            .map(|o| o.get_id().clone())
            .collect();
        client.decline(offer_ids, None).unwrap();
    }

    fn offers(&mut self, client: &SchedulerClient, offers: Vec<&Offer>) {
        // Offers are guaranteed to be for the same slave, and
        // there will be at least one.
        let slave_id = offers[0].get_slave_id();

        let requested_tasks = self.state_manager.request_list_requested_tasks();

        let mut tasks_to_start: Vec<TaskInfo> = vec![];
        let mut offers_to_decline: Vec<OfferID> = vec![];
        let mut offers_to_accept: Vec<OfferID> = vec![];

        let mut attribute_host = "";
        let mut attribute_node_name = "";
        let mut attribute_node_type = "";
        let mut attribute_node_function = "";

        let mut offer_cpus: f64 = 0.0;
        let mut offer_mem: f64 = 0.0;

        for offer in &offers {
            let mut found_match = false;

            for attribute in &mut offer.get_attributes().into_iter() {
                match attribute.get_name() {
                    "host" => attribute_host = attribute.get_text().get_value(),
                    "machine-name" => attribute_node_name = attribute.get_text().get_value(),
                    "machine-type" => attribute_node_type = attribute.get_text().get_value(),
                    "machine-function" => attribute_node_function = attribute.get_text().get_value(),
                    _ => {}
                }
            }

            for resource in &mut offer.get_resources().into_iter() {
                match resource.get_name() {
                    "mem" => offer_mem = resource.get_scalar().get_value(),
                    "cpus" => offer_cpus = resource.get_scalar().get_value(),
                    _ => {}
                }
            }

            println!("received offer from host: {}, name: {}, type: {}, function: {}",
                     attribute_host,
                     attribute_node_name,
                     attribute_node_type,
                     attribute_node_function);

            if !self.state_manager.request_is_node_active(attribute_node_name.to_string()) {
                self.state_manager.send_update_node(attribute_node_name.to_string(),
                                                    attribute_node_type.to_string(),
                                                    attribute_node_function.to_string(),
                                                    offer.get_slave_id().get_value().to_string())
            }

            for task_immutable in &requested_tasks {
                let mut task = task_immutable.clone();
                if task.node_name.len() > 0 && task.node_name != attribute_node_name {
                    continue;
                }

                if task.node_type.len() > 0 && task.node_type != attribute_node_type {
                    continue;
                }

                if task.node_function.len() > 0 && task.node_function != attribute_node_function {
                    continue;
                }

                if task.dependent_service.len() > 0 {
                    match self.state_manager.request_task_state(task.dependent_service.to_string()) {
                        TaskState::Running => {}
                        _ => continue,
                    }
                }

                if offer_cpus < task.cpu || offer_mem < task.memory {
                    continue;
                }

                println!("Starting {}, arguments: {:?}", task.name, task);
                self.state_manager.send_update_task_state(task.name.clone(), TaskState::Accepted);

                if task.node_type.len() > 0 || task.node_function.len() > 0 {
                    self.state_manager.send_update_task_node_name(task.name.clone(), attribute_node_name.to_string())
                }

                let name = &*format!("{}", task.name);
                let task_id = util::task_id(name);

                let mut command = CommandInfo::new();
                command.set_shell(false);

                if task.arguments.len() > 0 {
                    let elmts: Vec<&str> = task.arguments
                        .split(|c: char| c == ' ')
                        .filter(|s| !s.is_empty())
                        .collect();

                    let mut arguments: Vec<String> = vec![];

                    for elmt in elmts {
                        arguments.push(elmt.to_string());
                    }

                    command.set_arguments(protobuf::RepeatedField::from_vec(arguments));
                }

                let mut container = ContainerInfo::new();
                container.set_field_type(ContainerInfo_Type::DOCKER);

                let mut docker = ContainerInfo_DockerInfo::new();
                docker.set_image(task.image.clone());
                docker.set_privileged(task.privileged);

                match &*task.network_type {
                    "host" => docker.set_network(ContainerInfo_DockerInfo_Network::HOST),
                    "bridge" => docker.set_network(ContainerInfo_DockerInfo_Network::BRIDGE),
                    "none" => docker.set_network(ContainerInfo_DockerInfo_Network::NONE),
                    _ => {
                        let new_parameters = format!("{} --net={}", task.parameters, task.network_type.clone());
                        task.parameters = new_parameters.clone();
                    }
                }

                if task.parameters.len() > 0 {
                    let elmts: Vec<&str> = task.parameters
                        .split(|c: char| c == '-' || c == '=' || c == ' ')
                        .filter(|s| !s.is_empty())
                        .collect();

                    let mut count = 0;
                    let mut parameters: Vec<Parameter> = vec![];

                    loop {
                        let mut parameter = Parameter::new();
                        parameter.set_key(elmts[count].to_string());
                        count += 1;
                        parameter.set_value(elmts[count].to_string());
                        count += 1;

                        parameters.push(parameter);

                        if count >= elmts.len() {
                            break;
                        }
                    }

                    docker.set_parameters(protobuf::RepeatedField::from_vec(parameters));
                }

                if task.volumes.len() > 0 {
                    let mut volumes: Vec<Volume> = vec![];

                    for volume in task.volumes.clone() {
                        let mut definition = Volume::new();
                        definition.set_host_path(volume.host_path.to_string());
                        definition.set_container_path(volume.container_path.to_string());
                        match volume.read_only_mode {
                            true => definition.set_mode(Volume_Mode::RO),
                            false => definition.set_mode(Volume_Mode::RW),
                        }
                        volumes.push(definition);
                    }

                    container.set_volumes(protobuf::RepeatedField::from_vec(volumes));
                }

                container.set_docker(docker);

                let mem = util::scalar("mem", "*", task.memory);
                let cpus = util::scalar("cpus", "*", task.cpu);
                let resources = vec![mem, cpus];

                let task_info = util::task_info_for_container(name, &task_id, slave_id, &command, &container, resources);
                tasks_to_start.push(task_info);
                offers_to_accept.push(offer.get_id().clone());

                // we don't do any mem, cpu matching with offer yet,
                // assuming for now that we always have cpu and mem available ..
                found_match = true;
                break;
            }

            if !found_match {
                offers_to_decline.push(offer.get_id().clone());
            }

        }

        if tasks_to_start.len() > 0 {
            client.launch(offers_to_accept, tasks_to_start, None).unwrap();
        }

        if offers_to_decline.len() > 0 {
            client.decline(offers_to_decline, None).unwrap();
        }
    }

    fn rescind(&mut self, client: &SchedulerClient, offer_id: &OfferID) {
        println!("received rescind");
    }

    fn update(&mut self, client: &SchedulerClient, status: &TaskStatus) {
        println!("received update {:?} from {}",
                 status.get_state(),
                 status.get_task_id().get_value());

        let task_name = status.get_task_id().get_value().to_string();

        match status.get_state() {
            MesosTaskState::TASK_RUNNING => {
                let raw_data: Vec<u8> = Vec::from(status.get_data());
                let docker_inspect = String::from_utf8(raw_data).unwrap();
                utils::handle_inspect_data(&self.state_manager,
                                           &task_name,
                                           &docker_inspect,
                                           &status.get_slave_id().get_value().to_string());
                self.state_manager.send_update_task_state(task_name, TaskState::Running);
            }
            MesosTaskState::TASK_FINISHED => {
                // should only be valid for system-services .. change it
                // self.state_manager.send_update_task_state(task_name, TaskState::Requested);
            }            
            MesosTaskState::TASK_KILLED => {
                self.state_manager.send_remove_task_by_name(task_name);
            }            
            _ => {}
        }
    }

    fn message(&mut self, client: &SchedulerClient, slave_id: &SlaveID, executor_id: &ExecutorID, data: Vec<u8>) {
        println!("received message");
    }

    fn failure(&mut self,
               client: &SchedulerClient,
               slave_id: Option<&SlaveID>,
               executor_id: Option<&ExecutorID>,
               status: Option<i32>) {
        println!("received failure");
    }

    fn error(&mut self, client: &SchedulerClient, message: String) {
        println!("received error");
    }

    fn heartbeat(&mut self, client: &SchedulerClient) {
        println!("received heartbeat");
    }

    fn disconnected(&mut self) {
        println!("disconnected from scheduler");
    }
}
