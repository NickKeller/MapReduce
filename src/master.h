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
using masterworker::PingRequest;
using masterworker::MapRequest;
using masterworker::ReduceRequest;
using masterworker::TaskReply;
using masterworker::ShardPiece;
using masterworker::AssignTask;

enum WorkerStatus {MAP, REDUCE, IDLE, DEAD, ALIVE};

//some kind of map of shards to worker processes
class workerInfo{
	/*
	MAP - Currently running map task
	REDUCE - Currently running reduce task
	IDLE - Currenty waiting for a job
	DEAD - Worker is down
	*/
public:
	//the constructor
	workerInfo(const std::string& ip);
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
	//keeps track of the job id that this worker is working on
	std::string job_id;
	//the rpc call associated with the current task
	//std::unique_ptr<ClientAsyncResponseReader<TaskReply>> rpc;
};

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


		//store the spec
		MapReduceSpec spec;
		std::vector<FileShard> shards;
		std::vector<workerInfo> workers;

		//private functions
		WorkerStatus pingWorkerProcess(const std::string& ip_addr);
		void assignMapTask(workerInfo& worker);


};

workerInfo::workerInfo(const std::string& ip) : ip_addr(ip), state(IDLE), heartbeats(0) {}


/* CS6210_TASK: This is all the information your master will get from the framework.
	You can populate your other class data members here if you want */
Master::Master(const MapReduceSpec& mr_spec, const std::vector<FileShard>& file_shards) : spec(mr_spec), shards(file_shards) {
	//populate the workers vector
	for(int i = 0; i < mr_spec.worker_ipaddr_ports.size(); i++){
		std::string wrker = mr_spec.worker_ipaddr_ports.at(i);
		//have to assign a shard using a constructor like this. It will get reassigned later
		workerInfo worker(wrker);
		workers.push_back(worker);
		// workers.push_back({wrker, shards.at(0) , i, IDLE, 0, ""});
		std::cout << "Worker with ipAddr " << wrker << " added" << std::endl;
	}
}


/* CS6210_TASK: Here you go. once this function is called you will complete whole map reduce task and return true if succeeded */
bool Master::run() {
	bool map_complete = false;
	int last_shard_assigned = 0;
	//200ms heartbeat seems fair
	int heartbeat = 200;
	int num_shards_mapped = 0;
	while(!map_complete){
		//ping all worker processes
		//this can be done synchronously
		for(int i = 0; i < workers.size(); i++){
			//need to grab the address, otherwise we're not modifying the
			//actual workerInfo struct. Yay C++ semantics
			workerInfo* worker = &(workers.at(i));
			WorkerStatus state = pingWorkerProcess(worker->ip_addr);
			//state is either going to be ALIVE or DEAD
			if(state == ALIVE){
				worker->heartbeats++;
				std::cout << "Worker at " << worker->ip_addr << " is ALIVE for " << worker->heartbeats << " heartbeats" << std::endl;
			}
			else{
				std::cout << "Worker at " << worker->ip_addr << " is DEAD" << std::endl;
				worker->state = DEAD;
			}

		}
		//assign shards to alive worker processes, only if there are shards
		//left to assign
		for(int i = 0; i < workers.size(); i++){
			workerInfo& worker = workers.at(i);
			if(worker.state == IDLE && last_shard_assigned < shards.size()){
				//assign the next shard
				FileShard shard_to_assign = shards.at(last_shard_assigned);
				worker.shard = shard_to_assign;
				worker.shard_index = last_shard_assigned;
				worker.state = MAP;
				std::cout << "Assigned shard: " << worker.shard.shard_id << " to worker process: " << worker.ip_addr << std::endl;
				last_shard_assigned++;
				//fire off a map task, sync for now
				assignMapTask(worker);
				num_shards_mapped++;
			}
			else if(last_shard_assigned >= shards.size()){
				std::cout << "All shards assigned" << std::endl;
			}
		}

		//if all shards have been mapped, set the bool to complete
		if(num_shards_mapped == shards.size()){
			map_complete = true;
		}
		//wait for finish, reassign processes as needed
		std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat));
	}//end while
	//spin up reduce processes
	std::cout << "Master run" << std::endl;
	return true;
}

WorkerStatus Master::pingWorkerProcess(const std::string& ip_addr){
	WorkerStatus res = ALIVE;
	PingRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(ip_addr,grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	std::cout << "Pinging worker at " << ip_addr << std::endl;
	//can use synchronous rpc here, since this is just a ping
	Status status = stub->Ping(&context,request,&reply);

	//the workers aren't going to be doing anything if they're alive, so we only need to check if the
	//request failed
	if(!status.ok()){
		res = DEAD;
		std::cout << "WARNING: Worker " << ip_addr << " is not available" << std::endl;
	}

	return res;
}

void Master::assignMapTask(workerInfo& worker){
	MapRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker.ip_addr, grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	request.set_user_id("cs6210");
	//special thing to do for shards
	FileShard shard = worker.shard;
	for (int i = 0; i < shard.pieces.size(); i++) {
		file_key_offset* pair = shard.pieces.at(i);
		ShardPiece* piece = request.add_shard();
		piece->set_file_name(pair->filename);
		piece->set_start_index(pair->startOffset);
		piece->set_end_index(pair->endOffset);
	}
	std::string out_fname = "testoutfname_" + worker.shard.shard_id;

	request.set_out_fname(out_fname);
	std::string jobID = worker.ip_addr + "_map_" + std::to_string(worker.shard_index);
	request.set_job_id(jobID);
	worker.job_id = jobID;
	std::cout << "Assigning map task to " << worker.ip_addr << std::endl;
	Status status = stub->Map(&context,request,&reply);

	if(status.ok()){
		std::cout << "Successful map!" << std::endl;
		worker.state = IDLE;
		worker.heartbeats = 0;
	}
	else{
		std::cout << "Error in map!" << std::endl;
	}
}
