package Logger;
# Logger.pm
#---------------------------------------------------
#
# Basic module to log to file
#
#---------------------------------------------------
# Constructor for logger module

sub new{
    my $class = shift;
    my $args = shift;

    # Default is to create a new logfile, NOT to append to it
    my $append = 0;
    if (defined($args->{append})){
		$append = $args->{append};
    }

    my $disable = 0;
    if (defined($args->{disable})){
		$disable = $args->{disable};
    }

    my $self = { filename => $args->{filename},
		 		 append => $append,
		 		 line_no => 0,
		 		 disable => $disable
	     };

    bless($self,$class);
    $self->_init();
}

#---------------------------------------------------
# Initialise the log file

sub _init{
    my $self = shift;
    if (!$self->{disable}){
		$filename = $self->{filename};

		if ($self->{append}){
	    	# Open a file for appending in write mode
	    	open(LOG,">> $filename");
	    	select LOG;
	    	$| = 1;
	    	select STDOUT;
	    	$self->{log_handle} = *LOG;
		}
		else{
	    	# Create a new file in write mode
	    	open(LOG,"> $filename");
	    	select LOG;
	    	$| = 1;
	    	select STDOUT;
	    	$self->{log_handle} = *LOG;
		}
    }

    return $self;
}

#---------------------------------------------------
# Add to the log file

sub add{
    my $self = shift;
    if (!$self->{disable}){
		my $str = shift;

		my $handle = $self->{log_handle};
		my $timestamp = localtime(time);
		my $line_no = ++$self->{line_no};
               #print $handle "[$timestamp]: ". $str;
		print $handle "". $str
    }
}

#---------------------------------------------------
# Close log file

sub close{
    my $self = shift;
    if (!$self->{disable}){
		my $handle = $self->{log_handle};
		close($handle);
    }
}

#---------------------------------------------------
1;

