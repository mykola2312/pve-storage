package PVE::Storage::LunCmd::Ctld;

use strict;
use warnings;

use PVE::Tools qw(run_command);

sub get_base;
sub run_lun_command;

my $CONFIG_FILE = '/etc/ctl.conf';

my @ssh_opts = ('-o', 'BatchMode=yes');
my @ssh_cmd = ('/usr/bin/ssh', @ssh_opts);
my @scp_cmd = ('/usr/bin/scp', @ssh_opts);
my $id_rsa_path = '/etc/pve/priv/zfs';

my $split_lines = sub {
    my ($text) = @_;

    my @lines = split /(?<=\n)/, $text, -1;
    pop @lines if @lines && $lines[-1] eq '';

    return @lines;
};

my $normalize_timeout = sub {
    my ($timeout) = @_;
    return $timeout || 10;
};

my $get_target = sub {
    my ($scfg) = @_;
    return 'root@' . $scfg->{portal};
};

my $ssh_base = sub {
    my ($scfg) = @_;

    return [
        @ssh_cmd,
        '-i',
        "$id_rsa_path/$scfg->{portal}_id_rsa",
        $get_target->($scfg),
    ];
};

my $scp_base = sub {
    my ($scfg) = @_;

    return [
        @scp_cmd,
        '-i',
        "$id_rsa_path/$scfg->{portal}_id_rsa",
    ];
};

my $run_remote_command = sub {
    my ($scfg, $timeout, @remote_cmd) = @_;

    my $msg = '';
    my $cmd = [@{ $ssh_base->($scfg) }, @remote_cmd];

    my $output = sub {
        my $line = shift;
        $msg .= "$line\n";
    };

    run_command($cmd, outfunc => $output, timeout => $normalize_timeout->($timeout));

    return $msg;
};

my $run_remote_shell = sub {
    my ($scfg, $timeout, $script) = @_;
    return $run_remote_command->(
        $scfg,
        $timeout,
        'sh',
        '-c',
        PVE::Tools::shell_quote($script),
    );
};

my $scp_to_remote = sub {
    my ($scfg, $timeout, $local, $remote) = @_;

    my $cmd = [@{ $scp_base->($scfg) }, $local, $get_target->($scfg) . ":$remote"];
    run_command($cmd, timeout => $normalize_timeout->($timeout));

    return;
};

my $unquote = sub {
    my ($value) = @_;

    return undef if !defined($value);

    if ($value =~ /^"(.*)"$/s) {
        $value = $1;
        $value =~ s/\\"/"/g;
        $value =~ s/\\\\/\\/g;
    }

    return $value;
};

my $quote = sub {
    my ($value) = @_;

    $value =~ s/\\/\\\\/g;
    $value =~ s/"/\\"/g;

    return qq("$value");
};

my $brace_delta = sub {
    my ($line) = @_;

    my $tmp = $line;
    $tmp =~ s/"(?:[^"\\]|\\.)*"//g;
    $tmp =~ s/#.*$//;

    my $open = ($tmp =~ tr/{//);
    my $close = ($tmp =~ tr/}//);

    return $open - $close;
};

my $ensure_trailing_newline = sub {
    my ($text) = @_;

    $text .= "\n" if $text !~ /\n\z/;

    return $text;
};

my $block_indent = sub {
    my ($indent) = @_;

    return ($indent =~ /\t/) ? "$indent\t" : "$indent    ";
};

my $read_config = sub {
    my ($scfg, $timeout) = @_;

    my $config = eval { $run_remote_command->($scfg, $timeout, 'cat', $CONFIG_FILE) };
    if (my $err = $@) {
        die "Missing config file $CONFIG_FILE on $scfg->{portal}\n"
            if $err =~ /No such file or directory/;
        die $err;
    }

    die "Missing config file $CONFIG_FILE on $scfg->{portal}\n" if !$config;

    return $config;
};

my $parse_lun_block = sub {
    my ($lun, $raw) = @_;

    my $path;
    my $blocksize;

    for my $line ($split_lines->($raw)) {
        if ($line =~ /^\s*path\s+("(?:[^"\\]|\\.)+"|\S+)\s*(?:#.*)?$/) {
            $path = $unquote->($1);
        } elsif ($line =~ /^\s*blocksize\s+(\d+)\s*(?:#.*)?$/) {
            $blocksize = int($1);
        }
    }

    return {
        lun => int($lun),
        path => $path,
        blocksize => $blocksize,
        raw => $raw,
    };
};

my $parse_target_block = sub {
    my ($raw) = @_;

    my @lines = $split_lines->($raw);
    die "malformed target block in $CONFIG_FILE\n" if scalar(@lines) < 2;

    my $header = shift @lines;
    my $footer = pop @lines;
    my $preserved = '';
    my @luns;
    my %used;

    my $i = 0;
    while ($i < scalar(@lines)) {
        my $line = $lines[$i];

        if ($line =~ /^\s*lun\s+(\d+)\s*\{\s*(?:#.*)?$/) {
            my $lun = int($1);
            my $depth = $brace_delta->($line);
            my $block = $line;
            $used{$lun} = 1;
            $i++;

            while ($depth > 0) {
                die "unterminated lun block in $CONFIG_FILE\n" if $i >= scalar(@lines);
                $line = $lines[$i];
                $block .= $line;
                $depth += $brace_delta->($line);
                $i++;
            }

            push @luns, $parse_lun_block->($lun, $block);
            next;
        }

        if ($line =~ /^\s*lun\s+(\d+)\b/) {
            $used{int($1)} = 1;
        }

        $preserved .= $line;
        $i++;
    }

    my $indent = '    ';
    if ($preserved =~ /^([ \t]+)\S/m) {
        $indent = $1;
    } elsif (@luns && $luns[0]->{raw} =~ /^([ \t]+)lun\b/m) {
        $indent = $1;
    }

    return {
        header => $header,
        footer => $footer,
        preserved => $preserved,
        luns => \@luns,
        used => \%used,
        indent => $indent,
    };
};

my $parse_config = sub {
    my ($scfg, $config) = @_;

    my @parts;
    my $text = '';
    my $selected;

    my @lines = $split_lines->($config);
    my $i = 0;
    while ($i < scalar(@lines)) {
        my $line = $lines[$i];

        if ($line =~ /^\s*target\s+("(?:[^"\\]|\\.)+"|\S+)\s*\{\s*(?:#.*)?$/) {
            push @parts, { type => 'text', text => $text } if length($text);
            $text = '';

            my $name = $unquote->($1);
            my $depth = $brace_delta->($line);
            my $block = $line;
            $i++;

            while ($depth > 0) {
                die "unterminated target block in $CONFIG_FILE\n" if $i >= scalar(@lines);
                $line = $lines[$i];
                $block .= $line;
                $depth += $brace_delta->($line);
                $i++;
            }

            my $part = {
                type => 'target',
                name => $name,
                raw => $block,
            };

            if ($name eq $scfg->{target}) {
                die "$scfg->{target}: duplicate target definition in $CONFIG_FILE\n"
                    if $selected;
                $part->{selected} = 1;
                $selected = $part;
            }

            push @parts, $part;
            next;
        }

        $text .= $line;
        $i++;
    }

    push @parts, { type => 'text', text => $text } if length($text);

    die "$scfg->{target}: target not found in $CONFIG_FILE\n" if !$selected;

    $selected->{parsed} = $parse_target_block->($selected->{raw});

    return {
        parts => \@parts,
        selected => $selected,
    };
};

my $find_lun_by_path = sub {
    my ($parsed_target, $path) = @_;

    for my $entry (@{ $parsed_target->{luns} }) {
        next if !defined($entry->{path});
        return $entry if $entry->{path} eq $path;
    }

    return undef;
};

my $allocate_lun_number = sub {
    my ($parsed_target) = @_;

    for (my $lun = 0; $lun < 65536; $lun++) {
        return $lun if !$parsed_target->{used}->{$lun};
    }

    die "no free LUN numbers available for target\n";
};

my $parse_blocksize = sub {
    my ($blocksize) = @_;

    return undef if !defined($blocksize);
    return int($1) if $blocksize =~ /^(\d+)$/;

    if ($blocksize =~ /^(\d+)([KkMmGgTt])$/) {
        my ($value, $unit) = (int($1), lc($2));
        my $factor = {
            k => 1024,
            m => 1024 * 1024,
            g => 1024 * 1024 * 1024,
            t => 1024 * 1024 * 1024 * 1024,
        }->{$unit};
        return $value * $factor if $factor;
    }

    return undef;
};

my $parse_size = sub {
    my ($size) = @_;

    return undef if !defined($size);

    if ($size =~ /^(\d+)([KkMmGgTt])$/) {
        my ($value, $unit) = (int($1), lc($2));
        my $factor = {
            k => 1024,
            m => 1024 * 1024,
            g => 1024 * 1024 * 1024,
            t => 1024 * 1024 * 1024 * 1024,
        }->{$unit};

        return $value * $factor if $factor;
    }

    return undef;
};

my $render_lun_block = sub {
    my ($parsed_target, $entry) = @_;

    return $ensure_trailing_newline->($entry->{raw}) if defined($entry->{raw});

    my $indent = $parsed_target->{indent} || '    ';
    my $block_indent = $block_indent->($indent);
    my $raw = "${indent}lun $entry->{lun} {\n";

    $raw .= "${block_indent}blocksize $entry->{blocksize}\n" if $entry->{blocksize};
    $raw .= "${block_indent}path " . $quote->($entry->{path}) . "\n";
    $raw .= "${indent}}\n";

    return $raw;
};

my $render_target = sub {
    my ($parsed_target, $luns) = @_;

    my $raw = $parsed_target->{header} . $parsed_target->{preserved};
    my @entries = sort { $a->{lun} <=> $b->{lun} } @$luns;

    if (@entries) {
        $raw .= "\n" if $raw !~ /\n[ \t]*\n\z/;
        for my $entry (@entries) {
            $raw .= $render_lun_block->($parsed_target, $entry);
        }
    }

    $raw .= $parsed_target->{footer};

    return $raw;
};

my $render_config = sub {
    my ($parsed, $target_raw) = @_;

    my $config = '';
    for my $part (@{ $parsed->{parts} }) {
        if ($part->{type} eq 'text') {
            $config .= $part->{text};
        } elsif ($part->{selected}) {
            $config .= $target_raw;
        } else {
            $config .= $part->{raw};
        }
    }

    return $config;
};

my $parse_devlist = sub {
    my ($text) = @_;

    my @entries;
    my $current;

    for my $line (split /\n/, $text) {
        if ($line =~ /^\s*(\d+)\s+\S+\s+(\d+)\s+(\d+)\s+\S+\s+\S+\s*$/) {
            $current = {
                lun_id => int($1),
                size_blocks => int($2),
                blocksize => int($3),
            };
            push @entries, $current;
        } elsif ($current && $line =~ /^\s+(\w+)=(.*)$/) {
            $current->{$1} = $2;
        }
    }

    return \@entries;
};

my $find_ctl_lun = sub {
    my ($scfg, $timeout, $path) = @_;

    my $text = $run_remote_command->($scfg, $timeout, 'ctladm', 'devlist', '-v');
    for my $entry (@{ $parse_devlist->($text) }) {
        return $entry if defined($entry->{file}) && $entry->{file} eq $path;
    }

    return undef;
};

my $wait_for_ctl_lun = sub {
    my ($scfg, $timeout, $path, $should_exist) = @_;

    my $max_tries = $normalize_timeout->($timeout);
    $max_tries = 1 if $max_tries < 1;

    for (my $try = 0; $try < $max_tries; $try++) {
        my $entry = $find_ctl_lun->($scfg, 10, $path);
        return $entry if $should_exist && $entry;
        return 1 if !$should_exist && !$entry;
        sleep(1) if $try + 1 < $max_tries;
    }

    return undef;
};

my $wait_for_ctl_lun_size = sub {
    my ($scfg, $timeout, $path, $expected_size) = @_;

    my $max_tries = $normalize_timeout->($timeout);
    $max_tries = 1 if $max_tries < 1;

    for (my $try = 0; $try < $max_tries; $try++) {
        my $entry = $find_ctl_lun->($scfg, 10, $path);
        if ($entry && defined($entry->{size_blocks}) && defined($entry->{blocksize})) {
            my $actual_size = $entry->{size_blocks} * $entry->{blocksize};
            return $entry if $actual_size == $expected_size;
        }

        sleep(1) if $try + 1 < $max_tries;
    }

    return undef;
};

my $write_and_apply_config = sub {
    my ($scfg, $timeout, $config) = @_;

    my $local_tmp = "/tmp/ctl.conf.$$";
    my $remote_tmp = "/etc/ctl.conf.tmp.$$";
    my $remote_backup = "/etc/ctl.conf.bak.$$";

    open(my $fh, '>', $local_tmp) or die "Could not open file '$local_tmp' $!";
    print $fh $config;
    close $fh;

    eval {
        $scp_to_remote->($scfg, $timeout, $local_tmp, $remote_tmp);

        my $remote_q = PVE::Tools::shell_quote($remote_tmp);
        my $backup_q = PVE::Tools::shell_quote($remote_backup);
        my $live_q = PVE::Tools::shell_quote($CONFIG_FILE);

        my $script = <<"EOF";
ctld -t -f $remote_q || exit \$?
had_live=0
if [ -f $live_q ]; then
    cp $live_q $backup_q || exit \$?
    had_live=1
fi
mv $remote_q $live_q || exit \$?
if service ctld reload; then
    rm -f $backup_q
    exit 0
fi
rc=\$?
if [ "\$had_live" -eq 1 ]; then
    mv $backup_q $live_q || exit \$rc
else
    rm -f $live_q
fi
service ctld reload >/dev/null 2>&1 || true
exit \$rc
EOF

        my $chmod_tmp = 'chmod 600 ' . PVE::Tools::shell_quote($remote_tmp);
        $run_remote_shell->($scfg, $timeout, $chmod_tmp);
        $run_remote_shell->($scfg, $timeout, $script);
    };
    my $err = $@;

    unlink $local_tmp;

    eval {
        my $cleanup = 'rm -f '
            . join(' ', map { PVE::Tools::shell_quote($_) } $remote_tmp, $remote_backup);
        $run_remote_shell->($scfg, 10, $cleanup);
    };

    die $err if $err;

    return;
};

my $load_current_target = sub {
    my ($scfg, $timeout) = @_;

    my $config = $read_config->($scfg, $timeout);
    return $parse_config->($scfg, $config);
};

my $create_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $path = $params[0];

    my $parsed = $load_current_target->($scfg, $timeout);
    my $target = $parsed->{selected}->{parsed};

    die "$path: LUN already exists\n" if $find_lun_by_path->($target, $path);

    my $lun = $allocate_lun_number->($target);
    my @luns = @{ $target->{luns} };
    push @luns,
        {
            lun => $lun,
            path => $path,
            blocksize => $parse_blocksize->($scfg->{blocksize}),
        };

    my $config = $render_config->($parsed, $render_target->($target, \@luns));
    $write_and_apply_config->($scfg, $timeout, $config);

    die "$path: exported LUN did not appear after ctld reload\n"
        if !$wait_for_ctl_lun->($scfg, 10, $path, 1);

    return $path;
};

my $delete_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $path = $params[0];

    my $parsed = $load_current_target->($scfg, $timeout);
    my $target = $parsed->{selected}->{parsed};

    die "$path: LUN not found\n" if !$find_lun_by_path->($target, $path);

    my @luns = grep { !defined($_->{path}) || $_->{path} ne $path } @{ $target->{luns} };
    my $config = $render_config->($parsed, $render_target->($target, \@luns));
    $write_and_apply_config->($scfg, $timeout, $config);

    die "$path: exported LUN still present after ctld reload\n"
        if !$wait_for_ctl_lun->($scfg, 10, $path, 0);

    return $path;
};

my $import_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    return $create_lun->($scfg, $timeout, $method, @params);
};

my $modify_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my ($size, $path) = @params;

    my $parsed = $load_current_target->($scfg, $timeout);
    my $target = $parsed->{selected}->{parsed};

    die "$path: LUN not found\n" if !$find_lun_by_path->($target, $path);

    $run_remote_command->($scfg, $timeout, 'service', 'ctld', 'reload');

    my $entry = $wait_for_ctl_lun->($scfg, 10, $path, 1)
        or die "$path: exported LUN did not reappear after ctld reload\n";

    my $expected_size = $parse_size->($size);
    return $path if !defined($expected_size);

    my $refreshed = $wait_for_ctl_lun_size->($scfg, 10, $path, $expected_size)
        or die "$path: exported size mismatch after ctld reload\n";

    return $path;
};

my $add_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;

    # ctld exports the target-visible LUN mapping directly from the inline `lun N { ... }`
    # entries in /etc/ctl.conf, so create/import already establish the view.
    return '';
};

my $list_view = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $path = $params[0];

    my $parsed = $load_current_target->($scfg, $timeout);
    my $entry = $find_lun_by_path->($parsed->{selected}->{parsed}, $path);

    return defined($entry) ? $entry->{lun} : undef;
};

my $list_lun = sub {
    my ($scfg, $timeout, $method, @params) = @_;
    my $path = $params[0];

    my $parsed = $load_current_target->($scfg, $timeout);
    my $entry = $find_lun_by_path->($parsed->{selected}->{parsed}, $path);

    return defined($entry) ? $entry->{path} : undef;
};

my %lun_cmd_map = (
    create_lu => $create_lun,
    delete_lu => $delete_lun,
    import_lu => $import_lun,
    modify_lu => $modify_lun,
    add_view => $add_view,
    list_view => $list_view,
    list_lu => $list_lun,
);

sub run_lun_command {
    my ($scfg, $timeout, $method, @params) = @_;

    die "unknown command '$method'\n" if !exists $lun_cmd_map{$method};

    return $lun_cmd_map{$method}->($scfg, $timeout, $method, @params);
}

sub get_base {
    my ($scfg) = @_;
    return $scfg->{'zfs-base-path'} || '/dev/zvol';
}

1;
