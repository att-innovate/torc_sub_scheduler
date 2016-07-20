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

extern crate torc_dns_scheduler;

#[macro_use]
extern crate clap;

use std::thread;
use torc_dns_scheduler::state::StateManager;
use torc_dns_scheduler::scheduler::run_scheduler;
use torc_dns_scheduler::api::run_api;
use torc_dns_scheduler::health::run_health_checker;
use clap::{App, Arg};

fn main() {
    let matches = App::new("ToRC DNS Scheduler")
        .about("Orchestrates DNS Infrastructure on ToRC")
        .version(&crate_version!()[..])
        .arg(Arg::with_name("MASTER_IP")
            .short("m")
            .long("master")
            .required(true)
            .help("IP of master node")
            .takes_value(true))
        .arg(Arg::with_name("CONTROLLER_IP")
            .short("i")
            .long("controller")
            .required(false)
            .help("IP of controller")
            .takes_value(true))
        .arg(Arg::with_name("CONFIG")
            .short("c")
            .long("config")
            .required(false)
            .help("Path to configuration file")
            .takes_value(true))
        .get_matches();

    let master_ip = matches.value_of("MASTER_IP").unwrap();
    println!("Connecting to Master at: {}", master_ip);

    let mut controller_ip = matches.value_of("CONTROLLER_IP").unwrap_or("");
    match controller_ip.len() {
        0 => controller_ip = master_ip.clone(),
        _ => {}
    }
    println!("Controller IP set to : {}", controller_ip);

    let config_file = matches.value_of("CONFIG").unwrap_or("./config/config.yml");
    println!("Config file: {}", config_file);


    let state_manager = StateManager::new(master_ip.to_string(),
                                          controller_ip.to_string(),
                                          config_file.to_string());

    let api_state_manager = state_manager.clone();
    let _ = thread::Builder::new()
        .name("api".to_string())
        .spawn(move || run_api(&api_state_manager));

    let scheduler_state_manager = state_manager.clone();
    let _ = thread::Builder::new()
        .name("scheduler".to_string())
        .spawn(move || run_scheduler(&scheduler_state_manager));

    let health_state_manager = state_manager.clone();
    let health_check_runner = thread::Builder::new()
        .name("health".to_string())
        .spawn(move || run_health_checker(&health_state_manager))
        .unwrap();

    // wait forever
    let _ = health_check_runner.join();
}
