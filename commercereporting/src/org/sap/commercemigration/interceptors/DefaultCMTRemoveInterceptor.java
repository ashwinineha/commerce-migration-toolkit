package org.sap.commercemigration.interceptors;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import de.hybris.platform.core.model.ItemModel;
import de.hybris.platform.servicelayer.exceptions.ModelSavingException;
import de.hybris.platform.servicelayer.interceptor.InterceptorContext;
import de.hybris.platform.servicelayer.interceptor.RemoveInterceptor;
import de.hybris.platform.servicelayer.model.ModelService;
import de.hybris.platform.servicelayer.type.TypeService;
import de.hybris.platform.util.Config;
import java.util.Collections;
import java.util.List;
import javax.annotation.Nonnull;
import org.apache.commons.lang.StringUtils;
import org.sap.commercemigration.constants.CommercereportingConstants;
import org.sap.commercemigration.enums.ItemChangeType;
import org.sap.commercemigration.model.ItemDeletionMarkerModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultCMTRemoveInterceptor implements RemoveInterceptor<ItemModel> {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultCMTRemoveInterceptor.class);

  private static final boolean removeEnabled = Config.getBoolean(CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_REMOVE_ENABLED,true);

  private static final String COMMA_SEPERATOR = ",";

  private ModelService modelService;
  private TypeService typeService;

  @Override
  public void onRemove(@Nonnull final ItemModel model, @Nonnull final InterceptorContext ctx) {

    if (!removeEnabled ) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("CMT deletions is not enabled for ItemModel.");
      }
      return;
    }

    List<String> deletionsItemType = getListDeletionsItemType();

    if ( deletionsItemType == null || deletionsItemType.isEmpty()) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("No table defined to create a deletion record for CMT ");
      }
      return;
    }

    if (deletionsItemType.contains(model.getItemtype().toLowerCase())) {

      ItemDeletionMarkerModel idm = null;
      try {
        if(LOG.isDebugEnabled()){
          LOG.info("inside remove DefaultCMTRemoveInterceptor for" + String
              .valueOf(typeService.getComposedTypeForCode(model.getItemtype()).getTable()));
        }

        idm = modelService.create(ItemDeletionMarkerModel.class);
        fillInitialDeletionMarker(idm, model.getPk().getLong(),
            typeService.getComposedTypeForCode(model.getItemtype()).getTable());
        modelService.save(idm);

      } catch (ModelSavingException ex) {
        LOG.error("Exception during save for CMT table {} , PK : {} ", model.getItemtype(),
            model.getPk());
      }
    } else {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Table {} not defined for  CMT deletion record", model.getItemtype());
      }
    }
  }

  private void fillInitialDeletionMarker(final ItemDeletionMarkerModel marker, final Long itemPK,
      final String table) {
    Preconditions.checkNotNull(marker, "ItemDeletionMarker cannot be null in this place");
    Preconditions
        .checkArgument(marker.getItemModelContext().isNew(), "ItemDeletionMarker must be new");

    marker.setItemPK(itemPK);
    marker.setTable(table);
    marker.setChangeType(ItemChangeType.DELETED);
  }

  private  List<String> getListDeletionsItemType() {
    // TO DO change to static variable
    final String itemTypes = Config.getString(
        CommercereportingConstants.MIGRATION_DATA_INCREMENTAL_DELETIONS_ITEMTYPES, "");
    if (StringUtils.isEmpty(itemTypes)) {
      return Collections.emptyList();
    }
    List<String> result = Splitter.on(COMMA_SEPERATOR)
        .omitEmptyStrings()
        .trimResults()
        .splitToList(itemTypes.toLowerCase());

    return result;
  }

  public void setModelService(final ModelService modelService) {
    this.modelService = modelService;
  }

  public void setTypeService(final TypeService typeService) {
    this.typeService = typeService;
  }

}
