/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.yarn.entrypoint;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.JobClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.component.DefaultDispatcherResourceManagerComponentFactory;
import org.apache.flink.runtime.entrypoint.component.FileJobGraphRetriever;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.yarn.configuration.YarnConfigOptions;

import org.apache.hadoop.yarn.api.ApplicationConstants;

import java.io.IOException;
import java.util.Map;

/**
 * Entry point for Yarn per-job clusters.
 * yarn pre-job模式入口类
 */
public class YarnJobClusterEntrypoint extends JobClusterEntrypoint {

	public YarnJobClusterEntrypoint(Configuration configuration) {
		super(configuration);
	}

	@Override
	protected String getRPCPortRange(Configuration configuration) {
		return configuration.getString(YarnConfigOptions.APPLICATION_MASTER_PORT);
	}

	@Override
	protected DefaultDispatcherResourceManagerComponentFactory createDispatcherResourceManagerComponentFactory(Configuration configuration) throws IOException {
		return DefaultDispatcherResourceManagerComponentFactory.createJobComponentFactory(
			YarnResourceManagerFactory.getInstance(),
			FileJobGraphRetriever.createFrom(
					configuration,
					YarnEntrypointUtils.getUsrLibDir(configuration).orElse(null)));
	}

	// ------------------------------------------------------------------------
	//  The executable entry point for the Yarn Application Master Process
	//  for a single Flink job.
	// ------------------------------------------------------------------------

	public static void main(String[] args) {
		// startup checks and logging
		// 启动检查和日志，params：日志类，当前类名YarnJobClusterEntrypoint，参数
		// 这里调用的和CliFronted.main打印环境信息是同一个类
		EnvironmentInformation.logEnvironmentInfo(LOG, YarnJobClusterEntrypoint.class.getSimpleName(), args);
		// 注册日志类
		SignalHandler.register(LOG);
		// 这里启动了一个线程用于控制JVM安全关闭。
		JvmShutdownSafeguard.installAsShutdownHook(LOG);

		Map<String, String> env = System.getenv();
		// 获取key=pwd对应的工作目录
		final String workingDirectory = env.get(ApplicationConstants.Environment.PWD.key());
		// 检查工作目录
		// params: 判断条件、错误信息、参数
		Preconditions.checkArgument(
			workingDirectory != null,
			"Working directory variable (%s) not set",
			ApplicationConstants.Environment.PWD.key());

		try {
			// 打印yarn的环境信息
			YarnEntrypointUtils.logYarnEnvironmentInformation(env, LOG);
		} catch (IOException e) {
			LOG.warn("Could not log YARN environment information.", e);
		}
		// 加载配置
		Configuration configuration = YarnEntrypointUtils.loadConfiguration(workingDirectory, env);
		// 创建入口类
		YarnJobClusterEntrypoint yarnJobClusterEntrypoint = new YarnJobClusterEntrypoint(configuration);
		// 启动集群部署入口类
		ClusterEntrypoint.runClusterEntrypoint(yarnJobClusterEntrypoint);
	}
}
