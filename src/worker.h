#pragma once

#include <deque>
#include <mr_task_factory.h>
#include <grpc++/grpc++.h>
#include "mr_tasks.h"
#include "masterworker.grpc.pb.h"


using grpc::Server;
using grpc::ServerAsyncResponseWriter;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerCompletionQueue;
using grpc::Status;
using masterworker::MapRequest;
using masterworker::MapReply;
using masterworker::ReduceRequest;
using masterworker::ReduceReply;
using masterworker::PingRequest;
using masterworker::PingReply;
using masterworker::AssignTask;

extern std::shared_ptr<BaseMapper> get_mapper_from_task_factory(const std::string& user_id);
extern std::shared_ptr<BaseReducer> get_reducer_from_task_factory(const std::string& user_id);

/* CS6210_TASK: Handle all the task a Worker is supposed to do.
	This is a big task for this project, will test your understanding of map reduce */
class Worker {

	public:
		/* DON'T change the function signature of this constructor */
		Worker(std::string ip_addr_port);
		~Worker();

		/* DON'T change this function's signature */
		bool run();

		enum WorkerStatus { IDLE, MAPPING, REDUCING};
        WorkerStatus wrk_status;

        WorkerStatus get_status(){
            return Worker::wrk_status;
        }
        void set_status(WorkerStatus stat){
            Worker::wrk_status = stat;
        }
	private:
		/* NOW you can add below, data members and member functions as per the need of your implementation*/
		enum JobType { PING = 1, MAP = 2, REDUCE = 3};
		AssignTask::AsyncService task_service;
		std::unique_ptr<ServerCompletionQueue> task_cq;
		ServerContext task_ctx;
		std::unique_ptr<Server> task_server;
		class CallData {
			public:
				// Take in the "service" instance (in this case representing an asynchronous
				// server) and the completion queue "cq" used for asynchronous communication
				// with the gRPC runtime.
				CallData(AssignTask::AsyncService* service, ServerCompletionQueue* cq, JobType job_type_)
					: service_(service), cq_(cq), ping_responder(&ctx_), map_responder(&ctx_),reduce_responder(&ctx_),
                    status_(CREATE), job_type(job_type_) {
						// Invoke the serving logic right away.
                        Proceed();
					}//
                void Proceed(){
                        switch(job_type){
                            case(PING):
                                    PingProceed();
                                    break;
                            case(MAP):
                                    MapProceed();
                                    break;
                            case(REDUCE):
                                    ReduceProceed();
                                    break;
                        }//switch
                }
                void PingProceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        service_->RequestPing(&ctx_, &ping_req, &ping_responder, cq_, cq_,
                                this);
                        std::cout << "Ping CallData Created" << std::endl;
                    } else if (status_ == PROCESS) {
						//spawn a new ping calldata to handle new requests
						new CallData(service_,cq_,PING);
                        std::cout << "Pinging"<< std::endl;

                        status_ = FINISH;
                        ping_responder.Finish(ping_reply, Status::OK, this);
                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }//PingProceed
                void MapProceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        service_->RequestMap(&ctx_, &map_req, &map_responder, cq_, cq_,
                                this);
                        std::cout << "Mapper Call Data Created" << std::endl;
                    } else if (status_ == PROCESS) {
						//spawn a new map CallData to handle new maps
						new CallData(service_,cq_,MAP);
                        std::cout << "MAPPING"<< std::endl;
                        auto mapper = get_mapper_from_task_factory("cs6210");
                        //mapper->map("some_input_map");
						//print out the details of the map, then quit
						
                        status_ = FINISH;
                        map_responder.Finish(map_reply, Status::OK, this);
                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }//MapProceed
                void ReduceProceed() {
                    if (status_ == CREATE) {
                        // Make this instance progress to the PROCESS state.
                        status_ = PROCESS;

                        service_->RequestReduce(&ctx_, &reduce_req, &reduce_responder, cq_, cq_,
                                this);
                        std::cout << "Reduce CallData Created" << std::endl;
                    } else if (status_ == PROCESS) {
						//spawn a new map CallData to handle new maps
						new CallData(service_,cq_,REDUCE);
                        std::cout << "REDUCING"<< std::endl;
                        auto reducer = get_reducer_from_task_factory("cs6210");
                        //reducer->reduce("dummy",std::vector<std::string>({"1","1"}));

                        status_ = FINISH;
                        reduce_responder.Finish(reduce_reply, Status::OK, this);
                    } else {
                        GPR_ASSERT(status_ == FINISH);
                        delete this;
                    }
                }//ReduceProceed


			private:
				// The means of communication with the gRPC runtime for an asynchronous
				// server.
				AssignTask::AsyncService* service_;
				// The producer-consumer queue where for asynchronous server notifications.
				ServerCompletionQueue* cq_;
				// Context for the rpc, allowing to tweak aspects of it such as the use
				// of compression, authentication, as well as to send metadata back to the
				// client.
				ServerContext ctx_;

				// What we get from the client.
				PingRequest ping_req;
				PingReply ping_reply;
				MapRequest map_req;
				MapReply map_reply;
				ReduceRequest reduce_req;
				ReduceReply reduce_reply;

                JobType job_type;

				// The means to get back to the client.
				ServerAsyncResponseWriter<PingReply> ping_responder;
				ServerAsyncResponseWriter<MapReply> map_responder;
				ServerAsyncResponseWriter<ReduceReply> reduce_responder;

				// Let's implement a tiny state machine with the following states.
				enum CallStatus { CREATE, PROCESS, FINISH };
				CallStatus status_;  // The current serving state.
		};// Call Data
        std::deque<CallData> mini_workers;


}; // Worker Class

Worker::~Worker(){
    task_server->Shutdown();
    task_cq->Shutdown();
}


/* CS6210_TASK: ip_addr_port is the only information you get when started.
	You can populate your other class data members here if you want */
Worker::Worker(std::string ip_addr_port): wrk_status(IDLE) {
    ServerBuilder builder;
    builder.AddListeningPort(ip_addr_port, grpc::InsecureServerCredentials());
    builder.RegisterService(&task_service);
    task_cq = builder.AddCompletionQueue();
    task_server = builder.BuildAndStart();
    std::cout << "Worker listening on " << ip_addr_port << std::endl;

    // We only need 2 functions out of these workers. map/reduce and heartbeat
    // since the grpc example code is stateless, we needed to keep track that we are mapping / reducing and still alive

    //mini_workers.emplace_back(&task_service, task_cq.get(),&id,this);
    //mini_workers.emplace_back(&task_service, task_cq.get(),1,this);

}


/* CS6210_TASK: Here you go. once this function is called your woker's job is to keep looking for new tasks
	from Master, complete when given one and again keep looking for the next one.
	Note that you have the access to BaseMapper's member BaseMapperInternal impl_ and
	BaseReduer's member BaseReducerInternal impl_ directly,
	so you can manipulate them however you want when running map/reduce tasks*/
bool Worker::run() {
	/*  Below 5 lines are just examples of how you will call map and reduce
	Remove them once you start writing your own logic */
	// std::cout << "worker.run(), I 'm not ready yet" <<std::endl;
	// auto mapper = get_mapper_from_task_factory("cs6210");
	// mapper->map("I m just a 'dummy', a \"dummy line\"");
	// auto reducer = get_reducer_from_task_factory("cs6210");
	// reducer->reduce("dummy", std::vector<std::string>({"1", "1"}));
	// return true;
    void* tag;
    bool ok;

	//these are the three listeners that we will use for the ping, map, and reduce tasks
    new CallData(&task_service, task_cq.get(),PING);
    new CallData(&task_service, task_cq.get(),MAP);
    new CallData(&task_service, task_cq.get(),REDUCE);
    while(true) {
        GPR_ASSERT(task_cq->Next(&tag,&ok));
        GPR_ASSERT(ok);
        // handle the request, de-ref the tag, as the index to the deque
        static_cast<CallData*>(tag)->Proceed();
        //int index = static_cast<int>(reinterpret_cast<intptr_t>(tag));
    }
}
