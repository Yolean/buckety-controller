# s3 / naming-templates

**Scenario:** SPEC "Naming templates" for the s3 driver.

1. `spec.name` with `${backend.zone}` + `${namespace}` + `${name}`
   + `${label[...]}` resolves into `status.backendResourceName` and
   is sticky across label mutations.
2. A template referencing a missing label is rejected at admission.
3. A template that resolves to an *invalid bucket name* (uppercase
   and underscore, legal in the label value but not in S3) is
   rejected at admission by the driver's name validation.
