chdir "src/" || die "Couldn't change dir to src/\n$!\n";
my $return = system("make");
unless($return){
    chdir "../test/" || die "Couldn't change dir to test/\n$!\n";
    my $return2 = system("make");
}
