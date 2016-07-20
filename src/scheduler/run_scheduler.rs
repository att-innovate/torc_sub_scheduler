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

use mesos::{ProtobufCallbackRouter, SchedulerConf, run_protobuf_scheduler};
use scheduler::TorcScheduler;
use state::StateManager;
use mesos::proto::FrameworkID;

pub fn run_scheduler(state_manager: &StateManager) {
    state_manager.send_ping();

    let mut scheduler = TorcScheduler { state_manager: state_manager };
    let mut framework_id = FrameworkID::new();
    framework_id.set_value(state_manager.get_my_framework_id());

    let conf = SchedulerConf {
        master_url: format!("http://{}:5050", state_manager.get_master_ip()),
        user: "root".to_string(),
        name: state_manager.get_my_name(),
        framework_timeout: 0f64,
        implicit_acknowledgements: true,
        framework_id: Some(framework_id),
    };

    let mut router = ProtobufCallbackRouter {
        scheduler: &mut scheduler,
        conf: conf.clone(),
    };

    run_protobuf_scheduler(&mut router, conf)
}