package mutator

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/databricks/cli/bundle"
	"github.com/databricks/cli/bundle/config"
	"github.com/databricks/cli/bundle/config/resources"
	"github.com/databricks/cli/libs/log"
	"github.com/databricks/cli/libs/process"
	"github.com/databricks/cli/libs/python"
)

type pythonMutator struct {
	Phase string
}

func PythonMutator(phase string) bundle.Mutator {
	return &pythonMutator{Phase: phase}
}

func (m *pythonMutator) Name() string {
	return "PythonMutator"
}

func (m *pythonMutator) Apply(ctx context.Context, b *bundle.Bundle) error {
	if b.Config.Python == nil {
		log.Debugf(ctx, "No 'python' section, skipping")
		return nil
	}

	if b.Config.Python.Where == "" {
		log.Debugf(ctx, "No 'where' attribute in 'python' section, skipping")
		return nil
	}

	if len(b.Config.Python.Include) == 0 {
		log.Debugf(ctx, "No 'include' attribute in 'python' section, skipping")
		return nil
	}

	updatedRootConfig, err := m.runPythonMutator(ctx, &b.Config)

	if err != nil {
		return err
	}

	err = m.mergeMutatorChanges(ctx, &b.Config, updatedRootConfig)

	if err != nil {
		return err
	}

	return nil
}

func (m *pythonMutator) mergeMutatorChanges(ctx context.Context, rootConfig *config.Root, updatedRootConfig *config.Root) error {
	if updatedRootConfig.Resources.Jobs != nil {
		if rootConfig.Resources.Jobs == nil {
			rootConfig.Resources.Jobs = make(map[string]*resources.Job)
		}

		for resource_name, resource := range updatedRootConfig.Resources.Jobs {
			if resource.ConfigFilePath == "" {
				resource.ConfigFilePath = "resources/__generated__/fake.yml"
			}

			rootConfig.Resources.Jobs[resource_name] = resource
		}
	}

	rootConfigJson, err := json.Marshal(rootConfig)

	if err != nil {
		return err
	}

	updatedRootConfigJson, err := json.Marshal(updatedRootConfig)

	if err != nil {
		return err
	}

	if string(rootConfigJson) != string(updatedRootConfigJson) {
		return fmt.Errorf("python mutator unexpectedly changed config")
	}

	return nil
}

func (m *pythonMutator) runPythonMutator(ctx context.Context, rootConfig *config.Root) (*config.Root, error) {
	interpreter, err := detectInterpreter(ctx)

	if err != nil {
		return nil, err
	}

	args := []string{
		interpreter,
		"-m",
		"databricks.bundles.mutators",
		"--where",
		rootConfig.Python.Where,
		"--phase",
		m.Phase,
	}

	for _, include := range rootConfig.Python.Include {
		args = append(args, "--include", include)
	}

	rootConfigJson, err := json.Marshal(rootConfig)

	if err != nil {
		return nil, err
	}

	stdout, err := process.Background(
		ctx,
		args,
		process.WithStdinPipe(bytes.NewBuffer(rootConfigJson)),
	)

	updatedRootConfig := config.Root{}

	err = json.Unmarshal([]byte(stdout), &updatedRootConfig)

	if err != nil {
		return nil, err
	}

	return &updatedRootConfig, nil
}

func detectInterpreter(ctx context.Context) (string, error) {
	all, err := python.DetectInterpreters(ctx)
	if err != nil {
		return "", err
	}

	interpreter, err := all.AtLeast("3.10")
	if err != nil {
		return "", err
	}

	return interpreter.Path, nil
}
