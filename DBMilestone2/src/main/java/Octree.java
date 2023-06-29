package main.java;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Octree implements Serializable {
    private Node rootNode;
    private double p;
    private double q;
    private double r;

    public Octree(double p, double q, double r, double w, double h, double d) {
        this.rootNode = new Node(p, q, r, w, h, d);
    }

    public void addData(double p, double q, double r, Object info) {
        this.rootNode.addData(p, q, r, info);
    }

    public double getX() {
        return this.p;
    }

    public double getY() {
        return this.q;
    }

    public double getZ() {
        return this.r;
    }
    public List<Object> search(double p, double q, double r, double rad) {
        List<Object> output = new ArrayList<>();
        this.rootNode.search(p, q, r, rad, output);
        return output;
    }



    private static class Node implements Serializable {
        private double p, q, r, w, h, d;
        private List<Object> info;
        private Node[] subNodes;

        public Node(double p, double q, double r, double w, double h, double d) {
            this.p = p;
            this.q = q;
            this.r = r;
            this.w = w;
            this.h = h;
            this.d = d;
            this.info = new ArrayList<>();
            this.subNodes = new Node[8];
        }

        public void addData(double p, double q, double r, Object data) {
            if (this.subNodes[0] != null) {
                int indx = locateIndex(p, q, r);
                if (indx != -1) {
                    this.subNodes[indx].addData(p, q, r, data);
                    return;
                }
            }
            this.info.add(data);
            if (this.info.size() > 8) {
                if (this.subNodes[0] == null) {
                    divideNode();
                }
                int i = 0;
                while (i < this.info.size()) {
                    Object obj = this.info.get(i);
                    int indx = locateIndex(p, q, r);
                    if (indx != -1) {
                        this.subNodes[indx].addData(p, q, r, obj);
                        this.info.remove(i);
                    } else {
                        i++;
                    }
                }
            }
        }

        public void search(double p, double q, double r, double rad, List<Object> output) {
            double dx = p - this.p;
            double dy = q - this.q;
            double dz = r - this.r;
            double distanceSquared = dx * dx + dy * dy + dz * dz;
            if (distanceSquared <= rad * rad) {
                output.addAll(this.info);
            }
            if (this.subNodes[0] != null) {
                for (Node subNode : this.subNodes) {
                    subNode.search(p, q, r, rad, output);
                }
            }
        }

        private void divideNode() {
            double subW = this.w / 2;
            double subH = this.h / 2;
            double subD = this.d / 2;

            this.subNodes[0] = new Node(this.p, this.q, this.r, subW, subH, subD);
            this.subNodes[1] = new Node(this.p + subW, this.q, this.r, subW, subH, subD);
            this.subNodes[2] = new Node(this.p, this.q + subH, this.r, subW, subH, subD);
            this.subNodes[3] = new Node(this.p + subW, this.q + subH, this.r, subW, subH, subD);
            this.subNodes[4] = new Node(this.p, this.q, this.r + subD, subW, subH, subD);
            this.subNodes[5] = new Node(this.p + subW, this.q, this.r + subD, subW, subH, subD);
            this.subNodes[6] = new Node(this.p, this.q + subH, this.r + subD, subW, subH, subD);
            this.subNodes[7] = new Node(this.p + subW, this.q + subH, this.r + subD, subW, subH, subD);
        }

        private int locateIndex(double p, double q, double r) {
            int indx = -1;
            double midW = this.p + (this.w / 2);
            double midH = this.q + (this.h / 2);
            double midD = this.r + (this.d / 2);

            boolean lowerQuad = (q < midH);
            boolean upperQuad = (q >= midH);
            boolean leftQuad = (p < midW);
            boolean rightQuad = (p >= midW);
            boolean frontQuad = (r < midD);
            boolean backQuad = (r >= midD);

            if (leftQuad) {
                if (lowerQuad) {
                    if (frontQuad) {
                        indx = 0;
                    } else if (backQuad) {
                        indx = 4;
                    }
                } else if (upperQuad) {
                    if (frontQuad) {
                        indx = 2;
                    } else if (backQuad) {
                        indx = 6;
                    }
                }
            } else if (rightQuad) {
                if (lowerQuad) {
                    if (frontQuad) {
                        indx = 1;
                    } else if (backQuad) {
                        indx = 5;
                    }
                } else if (upperQuad) {
                    if (frontQuad) {
                        indx = 3;
                    } else if (backQuad) {
                        indx = 7;
                    }
                }
            }
            return indx;
        }
    }
}

