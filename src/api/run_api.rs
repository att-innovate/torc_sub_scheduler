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

use state::StateManager;
use iron::{Iron, IronResult, Request, Response};
use iron::mime::{Mime, SubLevel, TopLevel};
use hyper::header::AccessControlAllowOrigin;
use iron::status;
use router::Router;
use std::sync::Mutex;
use rustc_serialize::json;
use utils::read_task;

pub fn run_api(state_manager: &StateManager) {
    println!("api starting");
    state_manager.send_ping();

    let mut router = Router::new();
    router.get("/admin/ping", handle_ping);

    let nodes_state_manager = Mutex::new(state_manager.clone());
    router.get("/nodes",
               move |_r: &mut Request| handle_nodes(&nodes_state_manager));

    let services_metered_state_manager = Mutex::new(state_manager.clone());
    router.get("/services/metered",
               move |_r: &mut Request| handle_services_metered(&services_metered_state_manager));

    let services_running_state_manager = Mutex::new(state_manager.clone());
    router.get("/services/running",
               move |_r: &mut Request| handle_services_running(&services_running_state_manager));

    let service_state_manager = Mutex::new(state_manager.clone());
    router.get("/service",
               move |request: &mut Request| handle_service(&service_state_manager, request));

    let service_delete_state_manager = Mutex::new(state_manager.clone());
    router.delete("/service",
                  move |request: &mut Request| handle_service_delete(&service_delete_state_manager, request));

    let start_service_group_state_manager = Mutex::new(state_manager.clone());
    router.get("/start/group",
               move |request: &mut Request| handle_start_service_group(&start_service_group_state_manager, request));

    println!("API Server listening at: 3005");
    Iron::new(router).http("0.0.0.0:3005").unwrap();
}


#[derive(Clone, Debug, RustcEncodable)]
struct SimpleResponse {
    result: String,
}

fn handle_ping(_request: &mut Request) -> IronResult<Response> {
    Ok(Response::with((status::Ok, "pong")))
}

fn handle_nodes(state_manager: &Mutex<StateManager>) -> IronResult<Response> {
    let nodes = state_manager.lock().unwrap().request_list_nodes();
    let mut result = vec![];

    for node in nodes {
        if node.active {
            result.push(node);
        }
    }

    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());
    Ok(Response::with((content_type, status::Ok, json::encode(&result).unwrap())))
}

fn handle_services_metered(state_manager: &Mutex<StateManager>) -> IronResult<Response> {
    let tasks = state_manager.lock().unwrap().request_list_running_tasks();
    let mut result = vec![];

    for task in tasks {
        if task.is_metered {
            result.push(task);
        }
    }

    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());
    Ok(Response::with((content_type, status::Ok, json::encode(&result).unwrap())))
}

fn handle_services_running(state_manager: &Mutex<StateManager>) -> IronResult<Response> {
    let tasks = state_manager.lock().unwrap().request_list_running_tasks();
    let mut result = vec![];

    for task in tasks {
        if !task.is_job {
            result.push(task);
        }
    }

    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());
    Ok(Response::with((content_type, status::Ok, json::encode(&result).unwrap())))
}

fn handle_service(state_manager: &Mutex<StateManager>, request: &mut Request) -> IronResult<Response> {
    let url = request.url.clone().into_generic_url();
    let mut result = "".to_string();
    let query: String = match url.query {
        Some(q) => q.clone(),
        None => "".to_string(),
    };

    if !query.is_empty() && query.starts_with("id=") {
        let (_, id) = query.split_at(3);
        if !id.is_empty() {
            result = state_manager.lock().unwrap().request_task_name_by_id(id.to_string()).clone();
        }
    }

    let response = SimpleResponse { result: result };
    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());

    let mut res = Response::with((content_type, status::Ok, json::encode(&response).unwrap()));
    res.headers.set(AccessControlAllowOrigin::Any);
    Ok(res)
}

fn handle_service_delete(state_manager: &Mutex<StateManager>, request: &mut Request) -> IronResult<Response> {
    let url = request.url.clone().into_generic_url();
    let query: String = match url.query {
        Some(q) => q.clone(),
        None => "".to_string(),
    };

    if !query.is_empty() && query.starts_with("name=") {
        let (_, name) = query.split_at(5);
        if !name.is_empty() {
            state_manager.lock().unwrap().send_kill_task_by_name(name.to_string());
        }
    }

    let response = SimpleResponse { result: "done".to_string() };
    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());
    Ok(Response::with((content_type, status::Ok, json::encode(&response).unwrap())))
}

fn handle_start_service_group(state_manager: &Mutex<StateManager>, request: &mut Request) -> IronResult<Response> {
    let url = request.url.clone().into_generic_url();
    let query: String = match url.query {
        Some(q) => q.clone(),
        None => "".to_string(),
    };

    if !query.is_empty() && query.starts_with("name=") {
        let (_, name) = query.split_at(5);
        if !name.is_empty() {
            let config = state_manager.lock().unwrap().get_yaml();

            let service_groups = config["api"]["service-groups"].as_vec().unwrap();
            for service_group in service_groups {
                if service_group["name"].as_str().unwrap().to_string() == name {
                    let services = service_group["services"].as_vec().unwrap();
                    for service in services {
                        let task = read_task(service);
                        for cnt in 0..task.number_of_instances {
                            let mut task_name = task.name.clone();
                            if task.number_of_instances > 1 {
                                task_name = format!("{}-{}", task_name, cnt);
                            }

                            state_manager.lock().unwrap().send_start_task(&task_name,
                                                                          &task.image,
                                                                          &task.node_name,
                                                                          &task.node_type,
                                                                          &task.node_function,
                                                                          &task.dependent_service,
                                                                          &task.arguments,
                                                                          &task.parameters,
                                                                          &task.memory,
                                                                          &task.cpu,
                                                                          &task.volumes,
                                                                          &task.privileged,
                                                                          &task.sla,
                                                                          &task.is_metered,
                                                                          &false,
                                                                          &task.is_job,
                                                                          &task.network_type)
                        }
                    }
                }
            }
        }
    }

    let response = SimpleResponse { result: "done".to_string() };
    let content_type = Mime(TopLevel::Application, SubLevel::Json, Vec::new());
    Ok(Response::with((content_type, status::Ok, json::encode(&response).unwrap())))
}
