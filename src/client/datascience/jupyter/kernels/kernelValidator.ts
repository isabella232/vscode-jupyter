// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

import * as fastDeepEqual from 'fast-deep-equal';
import { inject, injectable } from 'inversify';
import { CancellationToken, CancellationTokenSource } from 'vscode';
import { IApplicationShell } from '../../../common/application/types';
import { IsLocalConnection, IsRawSupported } from '../../../common/constants';
import { traceInfo, traceVerbose } from '../../../common/logger';
import { IFileSystem } from '../../../common/platform/types';
import { ReadWrite, Resource } from '../../../common/types';
import { swallowExceptions } from '../../../common/utils/decorators';
import { DataScience } from '../../../common/utils/localize';
import { noop } from '../../../common/utils/misc';
import { IServiceContainer } from '../../../ioc/types';
import { PythonEnvironment } from '../../../pythonEnvironments/info';
import { sendTelemetryEvent } from '../../../telemetry';
import { Telemetry } from '../../constants';
import { IKernelFinder } from '../../kernel-launcher/types';
import { doesKernelSpecMatchInterpreter } from '../../notebookStorage/baseModel';
import {
    IJupyterKernelSpec,
    IJupyterNotebookProvider,
    IJupyterSessionManagerFactory,
    IKernelDependencyService
} from '../../types';
import { JupyterInterpreterService } from '../interpreter/jupyterInterpreterService';
import { isPythonKernelConnection } from './helpers';
import { KernelSelectionProvider } from './kernelSelections';
import { KernelService } from './kernelService';
import { IKernel, KernelConnectionMetadata, KernelSpecConnectionMetadata } from './types';

@injectable()
export class KernelValidator {
    private readonly kernelValidated = new WeakMap<IKernel, Promise<void>>();
    constructor(
        @inject(IsRawSupported) private readonly rawSupported: boolean,
        @inject(IsLocalConnection) private readonly isLocalConnection: boolean,
        @inject(IJupyterSessionManagerFactory) private jupyterSessionManagerFactory: IJupyterSessionManagerFactory,
        @inject(JupyterInterpreterService) private readonly jupyterInterpreter: JupyterInterpreterService,
        @inject(IFileSystem) private readonly fs: IFileSystem,
        @inject(KernelService) private readonly kernelService: KernelService,
        @inject(IKernelDependencyService) private readonly kernelDependencyService: IKernelDependencyService,
        @inject(KernelSelectionProvider) private readonly selectionProvider: KernelSelectionProvider,
        @inject(IApplicationShell) private readonly applicationShell: IApplicationShell,
        @inject(IKernelFinder) private readonly kernelFinder: IKernelFinder,
        @inject(IServiceContainer) private readonly serviceContainer: IServiceContainer
    ) {}
    public async validate(kernel: IKernel, token: CancellationToken): Promise<void> {
        if (this.kernelValidated.has(kernel)) {
            return this.kernelValidated.get(kernel)!;
        }
        const promise = this.validateLocalKernel(kernel.uri, kernel.kernelConnectionMetadata, token);
        this.kernelValidated.set(kernel, promise);
        await promise;
    }
    public async validateKernelSpecConnection(
        uri: Resource,
        kernel?: Readonly<KernelConnectionMetadata>,
        token: CancellationToken = new CancellationTokenSource().token
    ): Promise<void> {
        if (this.isLocalConnection || !kernel) {
            return;
        }
        if (kernel.kind !== 'startUsingKernelSpec') {
            return;
        }
        return this.validateLocalKernel(uri, kernel, token);
    }
    private async validateLocalKernel(
        uri: Resource,
        kernel: KernelConnectionMetadata,
        token: CancellationToken
    ): Promise<void> {
        if (kernel.interpreter && kernel.kind === 'startUsingPythonInterpreter') {
            await this.useInterpreterAsKernel(uri, kernel.interpreter, undefined, false, token);
        }
        if (this.rawSupported) {
            return;
        }
        await this.validateKernelsForJupyter(uri, kernel, token);
    }
    private async validateKernelsForJupyter(
        uri: Resource,
        kernel: KernelConnectionMetadata,
        token: CancellationToken
    ): Promise<void> {
        const kernelConnection = kernel;
        const interpreter = kernelConnection.interpreter;
        if (this.rawSupported || !isPythonKernelConnection(kernelConnection)) {
            return;
        }
        if (kernelConnection.kind !== 'startUsingKernelSpec') {
            return;
        }
        if (!interpreter) {
            return;
        }
        // We are only interested in python Kernels specs.

        // If we have a kernel spec and we know the interpreter, then check if this kernelspec is recognized by jupyter.
        // If not, then register this kernelspec as a real kernel spec.
        const jupyterNotebookProvider = this.serviceContainer.get<IJupyterNotebookProvider>(IJupyterNotebookProvider);
        const connection = await jupyterNotebookProvider.connect({
            resource: uri,
            disableUI: false,
            getOnly: false,
            kernelConnection: kernel,
            token
        });
        if (!connection) {
            return;
        }
        const sessionManager = await this.jupyterSessionManagerFactory.create(connection, true);
        const kernelspecs = await sessionManager.getKernelSpecs();
        const matchingKernelspec = kernelspecs.find((item) => item.name === kernelConnection.kernelSpec.name);

        if (!matchingKernelspec) {
            // We don't have a matching kernel, register a kernelspec for this.
            await this.useInterpreterAsKernel(
                uri,
                interpreter,
                undefined,
                false,
                token,
                kernelConnection.kernelSpec.specFile || kernelConnection.kernelSpec.display_name
            );
            return;
        }
        // Check if this is a kernelspec that we registered.
        if (doesKernelSpecMatchInterpreter(interpreter, matchingKernelspec)) {
            return;
        }
        const jupyterInterpreter = await this.jupyterInterpreter.getSelectedInterpreter();
        if (!jupyterInterpreter) {
            return;
        }
        if (matchingKernelspec.argv[0].trim().toLowerCase() === 'python') {
            // If the kernel spec has `python` as the first argument, then this means it will use the same
            // interpreter as the one that was used to start jupyter.
            // Check if the interpreters are the same.
            if (this.fs.areLocalPathsSame(interpreter.path, jupyterInterpreter.path)) {
                return;
            }

            // Ok, so the matching kernelspec we found in Jupyter is definitely not the same as the interpreter associated with the kernel the user has selected.
            // Hence we'll need to register a new kernel for this.
            // This happens when we display kernel specs from other interpreters that jupyter doesn't know about.
            // E.g. `Python 3` (default) kernel from some random Conda environment. The kernel spec will have argv=['python',...]
            // In such a case, the kernel spec is pointing to the conda environment, however if we use that with the Jupyter environment, we end up starting the wrong kernel.
            // Solution is to register the conda environment as a kernel.
            const generatedKernelSpec = await this.useInterpreterAsKernel(
                uri,
                interpreter,
                undefined,
                false,
                token,
                kernelConnection.kernelSpec.specFile || kernelConnection.kernelSpec.display_name
            );

            if (
                generatedKernelSpec?.kernelSpec.name &&
                generatedKernelSpec?.kernelSpec.name !== kernelConnection.kernelSpec.name
            ) {
                kernelConnection.kernelSpec.name = generatedKernelSpec?.kernelSpec.name;
            }
            if (generatedKernelSpec?.kernelSpec.specFile) {
                await this.updateKernelSpecWithOriginalKernelSpecInfo(
                    generatedKernelSpec.kernelSpec.specFile,
                    kernelConnection.kernelSpec.specFile || kernelConnection.kernelSpec.display_name
                );
            }
            return;
        } else if (
            // Check if the kernelspecs are the same.
            fastDeepEqual(
                {
                    argv: matchingKernelspec.argv,
                    name: matchingKernelspec.name,
                    display_name: matchingKernelspec.display_name,
                    env: matchingKernelspec.env,
                    metadata: matchingKernelspec.metadata
                },
                {
                    argv: kernelConnection.kernelSpec.argv,
                    name: kernelConnection.kernelSpec.name,
                    display_name: kernelConnection.kernelSpec.display_name,
                    env: kernelConnection.kernelSpec.env,
                    metadata: kernelConnection.kernelSpec.metadata
                }
            )
        ) {
            return;
        }
        // Looks like user has selected a kernel spec that Jupyter is not aware of.
        // Possible this kernel spec belongs to a Python environment (something other than the one used to start Jupyter).
        // Register this kernel spec for this environment.
        // This only
        const generatedKernelSpec = await this.kernelService.registerKernel(uri, interpreter, false, token);
        if (generatedKernelSpec?.name && generatedKernelSpec?.name !== kernelConnection.kernelSpec.name) {
            kernelConnection.kernelSpec.name = generatedKernelSpec.name;
            if (generatedKernelSpec.specFile) {
                await this.updateKernelSpecWithOriginalKernelSpecInfo(
                    generatedKernelSpec.specFile,
                    kernelConnection.kernelSpec.specFile || kernelConnection.kernelSpec.display_name
                );
            }
        }
    }
    /**
     * Use the provided interpreter as a kernel.
     * If `displayNameOfKernelNotFound` is provided, then display a message indicating we're using the `current interpreter`.
     * This would happen when we're starting a notebook.
     * Otherwise, if not provided user is changing the kernel after starting a notebook.
     */
    public async useInterpreterAsKernel(
        resource: Resource,
        interpreter: PythonEnvironment,
        displayNameOfKernelNotFound?: string,
        disableUI?: boolean,
        cancelToken?: CancellationToken,
        originalKernelSpecFile?: string
    ): Promise<KernelSpecConnectionMetadata | undefined> {
        let kernelSpec: IJupyterKernelSpec | undefined;

        const dependenciesInstalled = await this.kernelDependencyService.areDependenciesInstalled(
            interpreter,
            cancelToken
        );
        // If we're using Jupyter, then we don't care even if we have a kernel that matches this interpreter.
        // Its possible that kernel is in a location that the Jupyter server cannot find (i.e. its in a separate interpreter)
        if (dependenciesInstalled && this.rawSupported) {
            // Find the kernel associated with this interpreter.
            kernelSpec = await this.kernelFinder.findKernelSpec(resource, interpreter, cancelToken);

            if (kernelSpec) {
                traceVerbose(`ipykernel installed in ${interpreter.path}, and matching kernelspec found.`);
                // Make sure the environment matches.
                await this.kernelService.updateKernelEnvironment(interpreter, kernelSpec, cancelToken);

                // Notify the UI that we didn't find the initially requested kernel and are just using the active interpreter
                if (displayNameOfKernelNotFound && !disableUI) {
                    this.applicationShell
                        .showInformationMessage(
                            DataScience.fallbackToUseActiveInterpreterAsKernel().format(displayNameOfKernelNotFound)
                        )
                        .then(noop, noop);
                }

                sendTelemetryEvent(Telemetry.UseInterpreterAsKernel);
                return { kind: 'startUsingKernelSpec', kernelSpec, interpreter };
            }
            traceInfo(`ipykernel installed in ${interpreter.path}, no matching kernel found. Will register kernel.`);
        }

        // Try an install this interpreter as a kernel.
        try {
            kernelSpec = await this.kernelService.registerKernel(resource, interpreter, disableUI, cancelToken);
        } catch (e) {
            sendTelemetryEvent(Telemetry.KernelRegisterFailed);
            throw e;
        }

        // hideKernelFromKernelPicker
        if (originalKernelSpecFile && kernelSpec?.specFile) {
            await this.updateKernelSpecWithOriginalKernelSpecInfo(kernelSpec?.specFile, originalKernelSpecFile);
        }

        // If we have a display name of a kernel that could not be found,
        // then notify user that we're using the interpreter instead.
        if (displayNameOfKernelNotFound && !disableUI) {
            this.applicationShell
                .showInformationMessage(
                    DataScience.fallBackToRegisterAndUseActiveInterpeterAsKernel().format(
                        displayNameOfKernelNotFound,
                        interpreter.displayName!
                    )
                )
                .then(noop, noop);
        }

        // When this method is called, we know a new kernel may have been registered.
        // Lets pre-warm the list of local kernels (with the new list).
        this.selectionProvider.getKernelSelectionsForLocalSession(resource, cancelToken).ignoreErrors();

        if (kernelSpec) {
            return { kind: 'startUsingKernelSpec', kernelSpec, interpreter };
        }
    }
    @swallowExceptions()
    private async updateKernelSpecWithOriginalKernelSpecInfo(kernelSpecFile: string, originalKernelSpec: string) {
        if (!(await this.fs.localFileExists(kernelSpecFile))) {
            return;
        }
        // Update the metadata with details of the original kernel spec.
        // So that we don't display this in the kernel picker, & we have the information for diagnostics.
        const json: ReadWrite<IJupyterKernelSpec> = JSON.parse(await this.fs.readLocalFile(kernelSpecFile));
        json.metadata = json.metadata || {};
        json.metadata.originalKernelSpec = originalKernelSpec;
        await this.fs.writeLocalFile(kernelSpecFile, JSON.stringify(json, undefined, 4));
    }
}
