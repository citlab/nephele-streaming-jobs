/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package de.tuberlin.cit.livescale.job.task.channelselectors;

import de.tuberlin.cit.livescale.job.record.Packet;
import de.tuberlin.cit.livescale.job.record.VideoFrame;
import eu.stratosphere.nephele.configuration.Configuration;
import eu.stratosphere.nephele.io.ChannelSelector;

/**
 * @author bjoern
 * 
 */
public class ChannelSelectorProvider {

	public static final String KEY_CHANNEL_SELECTOR_TYPE = "KEY_CHANNEL_SELECTOR";

	public static final int BY_STREAM_CHANNEL_SELECTOR = 0;

	public static final int BY_STREAM_GROUP_CHANNEL_SELECTOR = 1;

	public static ChannelSelector<VideoFrame> getVideoFrameChannelSelector(
			Configuration taskConfiguration) {
		int selectorType = taskConfiguration.getInteger(
				KEY_CHANNEL_SELECTOR_TYPE, BY_STREAM_CHANNEL_SELECTOR);

		switch (selectorType) {
		case BY_STREAM_CHANNEL_SELECTOR:
			return new StreamVideoFrameChannelSelector();
		case BY_STREAM_GROUP_CHANNEL_SELECTOR:
			return new GroupVideoFrameChannelSelector();
		default:
			throw new RuntimeException(
					"Invalid video frame channel selector type. This is an error in task configuration");
		}
	}

	public static ChannelSelector<Packet> getPacketChannelSelector(
			Configuration taskConfiguration) {

		int selectorType = taskConfiguration.getInteger(
				KEY_CHANNEL_SELECTOR_TYPE, BY_STREAM_CHANNEL_SELECTOR);

		switch (selectorType) {
		case BY_STREAM_CHANNEL_SELECTOR:
			return new StreamPacketChannelSelector();
		case BY_STREAM_GROUP_CHANNEL_SELECTOR:
			return new GroupPacketChannelSelector();
		default:
			throw new RuntimeException(
					"Invalid video frame channel selector type. This is an error in task configuration");
		}
	}
}
