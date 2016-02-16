# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

try:
    from oslo_config import cfg
except ImportError:
    from oslo.config import cfg

logger_opts = [
    cfg.StrOpt("filename", default="/tmp/synergy.log",
               required=True),
    cfg.StrOpt("level", default="INFO", required=False),
    cfg.IntOpt("maxBytes", default=1048576),
    cfg.IntOpt("backupCount", default=100),
    cfg.StrOpt("formatter",
               default="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
               required=False)
]

cfg.CONF.register_opts(logger_opts, group="Logger")
