package kvdb

//"github.com/redis/go-redis/v9"

type RedisOptions struct {
	Host     string
	Port     int
	Password string
	DB       int
}

//var mdb *redis.Client
//var idb *redis.Client

//var ctx context.Context

/*
	 func InitRedis(o RedisOptions) {
		mdb = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", o.Host, o.Port), // Redis 服务器地址
			Password: o.Password,                           // 密码
			DB:       o.DB,                                 // 数据库编号
		})
		idb = redis.NewClient(&redis.Options{
			Addr:     fmt.Sprintf("%s:%d", o.Host, o.Port), // Redis 服务器地址
			Password: o.Password,                           // 密码
			DB:       o.DB + 1,                             // 数据库编号
		})
		// 测试连接
		//	ctx = context.Background()
	}
*/
type MemOptions struct {
	Dir string
	Mem bool
}

var memOptions MemOptions = MemOptions{
	Mem: true,
}

func InitMem(options MemOptions) {
	memOptions = options
}
