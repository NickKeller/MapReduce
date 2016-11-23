#!/usr/bin/env perl
use threads;
use strict;

#used to map ip addresses and ports
my %thread_map = ();

sub main{
    print "Welcome to Worker Process management.\n";
    while(1){
        print "Please Enter a command: ";
        my $input = <STDIN>;
        chomp $input;
        #supported commands are: start port, stop port
        #start will star a worker process on localhost:port, and stop will kill that process
        if($input =~ /start (\d+)/){
            my $addr = "localhost:$1";
            my $thr = threads->create(\&thread_routine,$addr);
            $thread_map{$addr} = $thr;
            print "Worker process created on $addr\n";
        }
        elsif(($input =~ /stop (\d+)/) || ($input =~ /stop (\w+)/)){
            if($1 =~ /all/){
                for my $key (keys %thread_map){
                    my $thr = $thread_map{$key};
                    $thr->kill('KILL')->detach();
                }
                print "Killed all worker processes\n";
            }
            else{
                my $addr = "localhost:$1";
                if($thread_map{$addr}){
                    my $thr = $thread_map{$addr};
                    $thr->kill('KILL')->detach();
                }
                else{
                    print "A worker process doesn't exist on $addr\n";
                }
            }
        }
        elsif($input =~ /quit/ || $input =~ /exit/){
            print "Goodbye!\n";
            exit(0);
        }
        else{
            print "Error: $input is not a supported\n";
            print "Available commands: start <port>, stop all, stop <port>, exit, quit\n";
        }

    }

}



sub thread_routine{
    $SIG{'KILL'} = sub{threads->exit();};
    my $addr = pop(@_);
    `./test/mr_worker $addr`;
}




&main();
