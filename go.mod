module gitlab.com/battler/modules

go 1.13

// replace gitlab.com/battler/models => ../../models

require (
	github.com/asaskevich/govalidator v0.0.0-20200428143746-21a406dcc535
	github.com/aws/aws-sdk-go v1.31.4
	github.com/go-sql-driver/mysql v1.5.0
	github.com/google/uuid v1.1.1
	github.com/jmoiron/sqlx v1.2.0
	github.com/juju/errors v0.0.0-20200330140219-3fe23663418f
	github.com/kataras/golog v0.0.15
	github.com/lib/pq v1.5.2
	github.com/mohae/deepcopy v0.0.0-20170929034955-c48cc78d4826
	github.com/prometheus/common v0.10.0
	github.com/sirupsen/logrus v1.6.0
	github.com/streadway/amqp v0.0.0-20200108173154-1c71cc93ed71
	github.com/tealeg/xlsx v1.0.5
	github.com/xor-gate/goexif2 v1.1.0
	gitlab.com/battler/models v0.0.0-20200524200721-adc3af00dae5
	gopkg.in/yaml.v2 v2.3.0
)
