# CMT - Deletion Support

CMT does support deletions, and it can be enabled for the transactional table.

## Approached for Deletions

Remove Interceptor, which will be enabled for the limited type code and only in a constraint violation.
* Activate the Delete Interceptor by defining an InterceptorMapping for each tracked item type.

```<bean id="defaultCMTRemoveInterceptorMapping"
    class="de.hybris.platform.servicelayer.interceptor.impl.InterceptorMapping">
    <property name="interceptor" ref="defaultCMTRemoveInterceptor"/>
    <property name="typeCode" value="Item" />
  </bean>
  ```

* Configurable property for the list of type codes where we should manage deletion
```
  # Provide the itemType for deletions
  migration.data.incremental.deletions.itemtype=Media,Employee
  migration.data.incremental.remove.enabled=true
```
* Dedicated item type for deleted records (separate table with PK).
```aidl
For now, it is supported through ItemDeletionMarker.
```
* Deletion activity is tied with incremental to avoid duplicates.

**Disclaimer**: Deletions will work with SL (legacy sync, legacy Impex, Service Layer Direct)
## When to use

* Not required to be enabled for all the tables, and few use-cases need to consider
 
  - In case of Constraint validation failure.
  - Deletion is triggered by application, e.g. removing the entry from a cart.
* Don't enable for audit table or task logs.
* It is covering deletions and migration together to avoid constraint validation.
* It can be toggle through properties.
