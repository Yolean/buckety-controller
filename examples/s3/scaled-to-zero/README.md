# s3 / scaled-to-zero

**Scenario:** SPEC "Off the data path", proven for the s3 driver.

A Buckety with `defaultAccess` mints `dial-tone-bucket`. The
controller Deployment is then scaled to zero, and only afterwards
is the consumer Job applied. The Job round-trips an object through
the bucket using nothing but the minted Secret, proving workloads
do not need the operator once credentials exist.

The controller is restored to one replica at the end so
subsequent scenarios see a running operator.
