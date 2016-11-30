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
	//the name of the output file that this is supposed to write to
	//used only for reduce tasks
	std::string output_file;
	//the TaskReply that this worker is going to receive information from
	TaskReply reply;
	//the tag used for the most recent request
	void* tag;
	//The status used for the reply
	Status rpc_status;

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
		std::vector<std::string> output_files;
		//the CompletionQueue used for map and reduce events
		CompletionQueue master_cq;

		//private functions
		WorkerStatus pingWorkerProcess(const workerInfo& worker);
		void assignMapTask(workerInfo& worker);
		void assignReduceTask(workerInfo& worker);
		workerInfo& findWorker(int* tag);


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
	//populate the output files vector
	for (size_t i = 0; i < spec.num_output_files; i++) {
		std::string fileName = spec.output_dir + "/output_" + std::to_string(i);
		output_files.push_back(fileName);
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
			workerInfo& worker = workers.at(i);
			WorkerStatus state = pingWorkerProcess(worker);
			//state is either going to be ALIVE or DEAD
			if(state == ALIVE){
				worker.heartbeats++;
				std::cout << "Worker at " << worker.ip_addr << " is ALIVE for " << worker.heartbeats << " heartbeats" << std::endl;
			}
			else{
				std::cout << "Worker at " << worker.ip_addr << " is DEAD" << std::endl;
				worker.state = DEAD;
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
				//fire off a map task
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
	bool reduce_complete = false;
	//fire up as many reduce tasks as nr_output_files
	int num_reduces_done = 0;
	int last_reduce_assigned = 0;
	while(!reduce_complete){
		//ping all worker processes
		//this can be done synchronously
		for(int i = 0; i < workers.size(); i++){
			//need to grab the address, otherwise we're not modifying the
			//actual workerInfo struct. Yay C++ semantics
			workerInfo& worker = workers.at(i);
			WorkerStatus state = pingWorkerProcess(worker);
			//state is either going to be ALIVE or DEAD
			if(state == ALIVE){
				worker.heartbeats++;
				std::cout << "Worker at " << worker.ip_addr << " is ALIVE for " << worker.heartbeats << " heartbeats" << std::endl;
			}
			else{
				std::cout << "Worker at " << worker.ip_addr << " is DEAD" << std::endl;
				worker.state = DEAD;
			}

		}
		//assign shards to alive worker processes, only if there are shards
		//left to assign
		for(int i = 0; i < workers.size(); i++){
			workerInfo& worker = workers.at(i);
			if(worker.state == IDLE && last_reduce_assigned < output_files.size()){
				//assign the next reduce task
				std::string output_file_to_assign = output_files.at(last_reduce_assigned);
				worker.output_file = output_file_to_assign;
				worker.shard_index = last_reduce_assigned;//can use shard_index as a double for this
				worker.state = REDUCE;
				std::cout << "Assigned reduce: " << worker.shard_index << " to worker process: " << worker.ip_addr << std::endl;
				last_reduce_assigned++;
				//fire off a map task
				assignReduceTask(worker);
				num_reduces_done++;
			}
			else if(last_reduce_assigned >= output_files.size()){
				std::cout << "All reduce tasks assigned" << std::endl;
			}
		}
		//if all shards have been mapped, set the bool to complete
		if(num_reduces_done == output_files.size()){
			reduce_complete = true;
		}
		//wait for finish, reassign processes as needed
		std::this_thread::sleep_for(std::chrono::milliseconds(heartbeat));
	}
	std::cout << "Master run" << std::endl;
	return true;
}

WorkerStatus Master::pingWorkerProcess(const workerInfo& worker){
	std::string ip_addr = worker.ip_addr;
	WorkerStatus res = ALIVE;
	PingRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(ip_addr,grpc::InsecureChannelCredentials()));
	ClientContext context;
	TaskReply reply;
	Status status;
	std::cout << "Pinging worker at " << ip_addr << std::endl;
	//sync here, because I need the state of all the workers before I can assign map tasks
	status = stub->Ping(&context,request,&reply);

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

	std::string jobID = worker.ip_addr + "_map_" + std::to_string(worker.shard_index);
	request.set_job_id(jobID);
	worker.job_id = jobID;
	std::cout << "Assigning map task to " << worker.ip_addr << std::endl;
	//sync
	//Status status;
	// status = stub->Map(&context,request,&(worker.reply));
	//async
	std::unique_ptr<ClientAsyncResponseReader<TaskReply>> rpc(stub->AsyncMap(&context, request, &master_cq));
	//also call finish
	std::cout << "Calling finish for job " << worker.job_id << std::endl;
	rpc->Finish(&(worker.reply), &(worker.rpc_status),(void*)(&(worker.shard_index)));
	//set the tag
	worker.tag = &(worker.shard_index);
	//block on a single response
	void* got_tag;
	bool ok = false;
	std::cout << "Calling next" << std::endl;
	GPR_ASSERT(master_cq.Next(&got_tag, &ok));
	//now, find the correct worker to use
	std::cout << "Getting worker" << std::endl;
	workerInfo& w = findWorker((int*)got_tag);
	std::cout << "Found worker: " << w.ip_addr << std::endl;
	GPR_ASSERT(got_tag == (void*)(w.tag));
	GPR_ASSERT(ok);

	if(worker.rpc_status.ok()){
		std::cout << "Successful map for " << worker.ip_addr << std::endl;
		worker.state = IDLE;
		worker.heartbeats = 0;
	}
	else{
		std::cout << "---------------ERROR IN MAP FOR " << worker.ip_addr << std::endl;
		std::cout << worker.rpc_status.error_code() << std::endl;
		std::cout << worker.rpc_status.error_message() << std::endl;
	}
	std::cout << "Returning from assignMapTask" << std::endl;
}

void Master::assignReduceTask(workerInfo& worker){
	ReduceRequest request;
	auto stub = AssignTask::NewStub(grpc::CreateChannel(worker.ip_addr, grpc::InsecureChannelCredentials()));
	ClientContext context;
	request.set_user_id("cs6210");
	request.set_output_file(worker.output_file);
	std::string jobID = worker.ip_addr + "_reduce_" + std::to_string(worker.shard_index);
	request.set_job_id(jobID);
	worker.job_id = jobID;
	std::cout << "Assigning map task to " << worker.ip_addr << std::endl;
	//sync
	//Status status;
	// status = stub->Reduce(&context,request,&(worker.reply));
	//async
	std::unique_ptr<ClientAsyncResponseReader<TaskReply>> rpc(stub->AsyncReduce(&context, request, &master_cq));
	//also call finish
	std::cout << "Calling finish for job " << worker.job_id << std::endl;
	rpc->Finish(&(worker.reply), &(worker.rpc_status),(void*)(&(worker.shard_index)));
	//set the tag
	worker.tag = &(worker.shard_index);
	//block on a single response
	void* got_tag;
	bool ok = false;
	std::cout << "Calling next" << std::endl;
	GPR_ASSERT(master_cq.Next(&got_tag, &ok));
	//now, find the correct worker to use
	std::cout << "Getting worker" << std::endl;
	workerInfo& w = findWorker((int*)got_tag);
	std::cout << "Found worker: " << w.ip_addr << std::endl;
	GPR_ASSERT(got_tag == (void*)(w.tag));
	GPR_ASSERT(ok);

	if(worker.rpc_status.ok()){
		std::cout << "Successful reduce for " << worker.ip_addr << std::endl;
		worker.state = IDLE;
		worker.heartbeats = 0;
	}
	else{
		std::cout << "---------------ERROR IN REDUCE FOR " << worker.ip_addr << std::endl;
		std::cout << worker.rpc_status.error_code() << std::endl;
		std::cout << worker.rpc_status.error_message() << std::endl;
	}
	std::cout << "Returning from assignReduceTask" << std::endl;
}

workerInfo& Master::findWorker(int* tag){
	for (size_t i = 0; i < workers.size(); i++) {
		workerInfo& worker = workers.at(i);
		if(worker.tag == tag){
			return worker;
		}
	}
	workerInfo w("ERROR");
	return w;
}
