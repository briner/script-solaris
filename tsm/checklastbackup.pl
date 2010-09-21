#!/usr/bin/perl

# crontab:
# 0 9 * * * /tsm/bin/checklastbackup.pl > /tmp/checklastbackup.log 2>&1

use strict;
my ($result,$i,$start,$line);
my @tmp;

my @output = `/opt/tivoli/tsm/client/ba/bin/dsmadmc -se=rgt -id=admintsm -pa=rivella -tabdelimited q fi f=d`;
my $exit_value=$? >> 8;

$start=0;
foreach $line (@output) {
  chomp($line);  
  next if ($line =~ /^$/);
  if ($line eq "ANS8002I Highest return code was 0\.") {last;};
  if ($start == "1") {
    @tmp = split(/\t/,$line);
    $result = $result."$tmp[0]\t$tmp[12]\t\t$tmp[1]\n";
  };
  if ($line =~ m/^ANS8000I Server command\: \'q fi f\=d\'$/) {$start=1};
}

#print "$exit_value\n";

my $msg = "Node\tLast Backup\tFilespace Name\nName\t(in days)\n----\t-----------\t--------------\n".$result;
my $to = 'Fabrice.DiPasquale@unige.ch, Jean-Marc.Naef@unige.ch, Remy.Papillon@unige.ch, Massimo.Usel@unige.ch, Dominique.Petitpierre@unige.ch, Robin.Schaffar@unige.ch';
my $subj = '[TSM@RGT] : Informations de backup';

open my $pipe, '|-', '/usr/bin/mailx', '-s', $subj, $to
or die "can't open pipe to mailx: $!\n";
print $pipe $msg;
close $pipe;
die "mailx exited with a non-zero status: $?\n" if $?;

