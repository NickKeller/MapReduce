#pragma once

#include <thread>
#include <chrono>
#include "mapreduce_spec.h"
#include "file_shard.h"
#include <mr_task_factory.h>
#include <grpc++/grpc++.h>
#include "masterworker.grpc.pb.h"

using grpc::ClientContext;
using grpc::CompletionQueue;
using grpc::ClientAsyncResponseReader;
using grpc::Status;
using grpc::Channel;
using masterworker::TaskRequest;
using masterworker::TaskReply;
using masterworker::AssignTask;


/* CS6210_TASK: Handle all the bookkeeping that Master is supposed to do.
	This is probably the biggest task for this project, will test your understanding of map reduce */
class Master {

	public:
		/* DON'T change the function signature of this constructor */
		Master(const MapReduceSpec&, const std::vector<FileShard>&);

		/* DON'T change this function's signature */
		bool run();


	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/

		/*
		MAP - Currently running map task
		REDUCE - Currently running reduce task
		IDLE - Currenty waiting for a job
		DEAD - Worker is down
		*/
		enum WorkerStatus {MAP, REDUCE, IDLE, DEAD};

		//some kind of map of shards to worker processes
		struct workerInfo{
			public:
				//the ip address of the worker
				std::string ip_addr;
				//the specific shard that it's working on
				FileShard shard;
				//the index of the FileShard in the FileShard vector, probably going to use this one more
				int shard_index;
				//keeps the state of the worker
				WorkerStatus state;
				//keeps track of the amount of heartbeats that it has been running for
				int heartbeats;
		};
		//store the spec
		MapReduceSpec spec;
		std::vector<FileShard> shards;
		std::vector<workerInfo> workers;

		//private functions
		WorkerStatus pingWorkerProcess(const std::string& ip_addr);

};


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : spec(mr_spec), shards(file_shards) {
	//populate the workers vector
	for(int i = 0; i < mr_spec.worker_ipaddr_ports.size(); i++){
		std::string wrker = mr_spec.worker_ipaddr_ports.at(i);
		//have to assign a shard using a constructor like this. It will get reassigned later
		workers.push_back({wrker, shards.at(0) , i, pingWorkerProcess(wrker), 0});
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	bool map_complete = false;
	int last_shard_assigned = 0;
	int heartbeat = 200;
	while(!map_complete){
		//ping all worker processes
		for(int i = 0; i < workers.size(); i++){
			workerInfo worker = workers.at(i);
			worker.state = pingWorkerProcess(worker.ip_addr);
			std::string status;
			switch (worker.state) {
				case IDLE:
					status = "IDLE";
					break;
				case MAP:
					status = "MAP";
					break;
				case REDUCE:
					status = "REDUCE";
					break;
				case DEAD:
					status = "DEAD";
					break;
				default:
					status = "UNKNOWN";
					break;
			}
			std::cout << "Worker at " << worker.ip_addr << " is now " << status << std::endl;
			std::cout << "/* message */" << std::endl;
		}
		//assign shards to alive worker processes
		for(auto worker : workers){
			if(worker.state == IDLE){
				//assign the next shard
				FileShard shard_to_assign = shards.at(last_shard_assigned);
				worker.shard = shard_to_assign;
				worker.shard_index = last_shard_assigned;
				std::cout << "Assigned shard: " << worker.shard.shard_id << " to worker process: " << worker.ip_addr << std::endl;
				last_shard_assigned++;
			}
		}
		//possibly set up a ping every X seconds
		//wait for finish, reassign processes as needed
		std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat));
	}//end while
	//spin up reduce processes
	std::cout << "Master run" << std::endl;
	return true;
}

Master::WorkerStatus Master::pingWorkerProcess(const std::string& ip_addr){
	WorkerStatus res = IDLE;
	TaskRequest request;
	request.set_job(masterworker::TaskRequest_JobType_ALIVE);
	auto stub = AssignTask::NewStub(grpc::CreateChannel(ip_addr,grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	//can use synchronous rpc here, because we're in the constructor
	Status status = stub->DoTask(&context,request,&reply);

	//the workers aren't going to be doing anything if they're alive, so we only need to check if the
	//request failed
	if(!status.ok()){
		res = DEAD;
		std::cout << "WARNING: Worker " << ip_addr << " is not available" << std::endl;
	}

	return res;
}
