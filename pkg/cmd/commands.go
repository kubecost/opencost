package cmd

import (
	"fmt"
	"os"
	"strings"

	"github.com/opencost/opencost/core/pkg/log"
	"github.com/opencost/opencost/pkg/cmd/agent"
	"github.com/opencost/opencost/pkg/cmd/costmodel"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	// commandRoot is the root command used to route to sub-commands
	commandRoot string = "root"

	// CommandCostModel is the command used to execute the metrics emission and ETL pipeline
	CommandCostModel string = "cost-model"

	// CommandAgent executes the application in agent mode, which provides only metrics exporting.
	CommandAgent string = "agent"
)

// Execute runs the root command for the application. By default, if no command argument is provided,
// on the command line, the cost-model is executed by default.
//
// This function accepts a costModelCmd and agentCmd parameters to provide support for alternate
// implementations for cost-model and/or agent. If the passed in costModelCmd and/or agentCmd are nil,
// then the respective defaults from opencost will be used.
//
// Any additional commands passed in will be added to the root command.
func Execute(costModelCmd *cobra.Command, cmds ...*cobra.Command) error {
	// use the open-source cost-model if a command is not provided
	if costModelCmd == nil {
		costModelCmd = newCostModelCommand()
	}

	// validate the commands being passed
	if err := validate(costModelCmd, CommandCostModel); err != nil {
		return err
	}

	// prepend the -agent command and create a new root command
	rootCmd := newRootCommand(costModelCmd, cmds...)

	// in the event that no directive/command is passed, we want to default to using the cost-model command
	// cobra doesn't provide a way within the API to do this, so we'll prepend the command if it is omitted.
	if len(os.Args) > 1 {
		// try to find the sub-command from the arguments, if there's an error or the command _is_ the
		// root command, prepend the default command
		pCmd, _, err := rootCmd.Find(os.Args[1:])
		if err != nil || pCmd.Use == rootCmd.Use {
			rootCmd.SetArgs(append([]string{CommandCostModel}, os.Args[1:]...))
		}
	} else {
		rootCmd.SetArgs([]string{CommandCostModel})
	}

	return rootCmd.Execute()
}

// newRootCommand creates a new root command which will act as a sub-command router for the
// cost-model application.
func newRootCommand(costModelCmd *cobra.Command, cmds ...*cobra.Command) *cobra.Command {
	cmd := &cobra.Command{
		Use:          commandRoot,
		SilenceUsage: true,
	}

	// Add our persistent flags, these are global and available anywhere
	cmd.PersistentFlags().String("log-level", "info", "Set the log level")
	cmd.PersistentFlags().String("log-format", "pretty", "Set the log format - Can be either 'JSON' or 'pretty'")
	cmd.PersistentFlags().Bool("disable-log-color", false, "Disable coloring of log output")

	viper.BindPFlag("log-level", cmd.PersistentFlags().Lookup("log-level"))
	viper.BindPFlag("log-format", cmd.PersistentFlags().Lookup("log-format"))
	viper.BindPFlag("disable-log-color", cmd.PersistentFlags().Lookup("disable-log-color"))

	// Setup viper to read from the env, this allows reading flags from the command line or the env
	// using the format 'LOG_LEVEL'
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))

	// add the modes of operation
	cmd.AddCommand(
		append([]*cobra.Command{
			costModelCmd,
			newAgentCommand(),
		}, cmds...)...,
	)

	return cmd
}

// default open-source cost-model command
func newCostModelCommand() *cobra.Command {
	opts := &costmodel.CostModelOpts{}

	cmCmd := &cobra.Command{
		Use:   CommandCostModel,
		Short: "Cost-Model metric exporter and data aggregator.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Init logging here so cobra/viper has processed the command line args and flags
			// otherwise only envvars are available during init
			log.InitLogging(true)
			return costmodel.Execute(opts)
		},
	}

	// TODO: We could introduce a way of mapping input command-line parameters to a configuration
	// TODO: object, and pass that object to the agent Execute()
	// cmCmd.Flags().<Type>VarP(&opts.<Property>, "<flag>", "<short>", <default>, "<usage>")

	return cmCmd
}

func newAgentCommand() *cobra.Command {
	opts := &agent.AgentOpts{}

	agentCmd := &cobra.Command{
		Use:   CommandAgent,
		Short: "Agent mode operates as a metric exporter only.",
		RunE: func(cmd *cobra.Command, args []string) error {
			// Init logging here so cobra/viper has processed the command line args and flags
			// otherwise only envvars are available during init
			log.InitLogging(true)
			return agent.Execute(opts)
		},
	}

	// TODO: We could introduce a way of mapping input command-line parameters to a configuration
	// TODO: object, and pass that object to the agent Execute()
	// agentCmd.Flags().<Type>VarP(&opts.<Property>, "<flag>", "<short>", <default>, "<usage>")

	return agentCmd
}

// validate checks the command's use to see if it matches an expected command name.
func validate(cmd *cobra.Command, command string) error {
	if cmd.Use != command {
		return fmt.Errorf("Incompatible '%s' command provided to run-time.", command)
	}
	return nil
}
