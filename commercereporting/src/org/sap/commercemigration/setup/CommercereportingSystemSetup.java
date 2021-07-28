/*
 * Copyright (c) 2019 SAP SE or an SAP affiliate company. All rights reserved.
 */
package org.sap.commercemigration.setup;

import de.hybris.platform.core.initialization.SystemSetup;

import org.sap.commercemigration.constants.CommercereportingConstants;


@SystemSetup(extension = CommercereportingConstants.EXTENSIONNAME)
public class CommercereportingSystemSetup
{

	@SystemSetup(process = SystemSetup.Process.INIT, type = SystemSetup.Type.ESSENTIAL)
	public void createEssentialData()
	{
      //
	}

}
