require "../commands"

# Deferred command objects are ones that defer the execution of the command in
# some way. For example, `Pipeline` defers receiving the results of a command
# and `Transaction` defers the execution of sent commands by the server until
# the server receives the `EXEC` command.
#
# `Deferred` objects may return anything from their `run` method.
module Redis::Commands::Deferred
end
