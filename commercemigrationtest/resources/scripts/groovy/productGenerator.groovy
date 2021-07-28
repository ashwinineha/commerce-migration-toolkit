package scripts.groovy


import de.hybris.platform.core.PK
import de.hybris.platform.core.model.product.ProductModel
import de.hybris.platform.servicelayer.model.ModelService

int numberOfProductsToCreate = 10

ModelService modelService = spring.getBean "modelService"

ProductModel template = modelService.get(PK.fromLong(8796110618625));

int start = 0;
for (int i = start; i < start + numberOfProductsToCreate; i++) {

    ProductModel product = modelService.clone(template)

    product.setCode("MassProduct" + i)

    product.setName("Groovy Product " + i)

    product.setDescription("Product Created via Groovy Script")

    //fill in some binary types
    product.setBuyerIDS(template.getBuyerIDS());
    product.setSpecialTreatmentClasses(template.getSpecialTreatmentClasses());

    modelService.attach(product)

}

modelService.saveAll();
